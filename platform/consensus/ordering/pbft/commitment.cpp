/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "platform/consensus/ordering/pbft/commitment.h"

#include <glog/logging.h>
#include <unistd.h>

#include "common/utils/utils.h"
#include "platform/consensus/ordering/pbft/transaction_utils.h"

namespace resdb {

Commitment::Commitment(const ResDBConfig& config,
                       MessageManager* message_manager,
                       ReplicaCommunicator* replica_communicator,
                       SignatureVerifier* verifier)
    : config_(config),
      message_manager_(message_manager),
      stop_(false),
      replica_communicator_(replica_communicator),
      verifier_(verifier) {
  executed_thread_ = std::thread(&Commitment::PostProcessExecutedMsg, this);
  global_stats_ = Stats::GetGlobalStats();
  duplicate_manager_ = std::make_unique<DuplicateManager>(config);
  message_manager_->SetDuplicateManager(duplicate_manager_.get());

  global_stats_->SetProps(
      config_.GetSelfInfo().id(), config_.GetSelfInfo().ip(),
      config_.GetSelfInfo().port(), config_.GetConfigData().enable_resview(),
      config_.GetConfigData().enable_faulty_switch());
  global_stats_->SetPrimaryId(message_manager_->GetCurrentPrimary());
}

Commitment::~Commitment() {
  stop_ = true;
  if (executed_thread_.joinable()) {
    executed_thread_.join();
  }
}

void Commitment::SetPreVerifyFunc(
    std::function<bool(const Request& request)> func) {
  pre_verify_func_ = func;
}

void Commitment::SetNeedCommitQC(bool need_qc) { need_qc_ = need_qc; }

// Handle the user request and send a pre-prepare message to others.
// TODO if not a primary, redicet to the primary replica.
int Commitment::ProcessNewRequest(std::unique_ptr<Context> context,
                                  std::unique_ptr<Request> user_request) {
  // Verify the request has a valid signature.
  if (context == nullptr || context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject";
    return -2;
  }

  // Check if request was already executed previously
  if (uint64_t seq =
          duplicate_manager_->CheckIfExecuted(user_request->hash())) {
    LOG(ERROR) << "This request is already executed with seq: " << seq;
    user_request->set_seq(seq);
    message_manager_->SendResponse(std::move(user_request));
    return -2;
  }
  // Check if this node is the primary replica.
  if (config_.GetSelfInfo().id() != message_manager_->GetCurrentPrimary()) {
    // LOG(ERROR) << "current node is not primary. primary:"
    //            << message_manager_->GetCurrentPrimary()
    //            << " seq:" << user_request->seq()
    //            << " hash:" << user_request->hash();
    LOG(INFO) << "NOT PRIMARY, Primary is "
              << message_manager_->GetCurrentPrimary();
    // Forward request to the primary.
    // replica_communicator_->SendMessage(*user_request,
    //  message_manager_->GetCurrentPrimary());
    // {
    //   std::lock_guard<std::mutex> lk(rc_mutex_);
    //   request_complained_.push(
    //       std::make_pair(std::move(context), std::move(user_request)));
    // }
    // return -3;
  }
  // LOG(INFO) << "Starting 3pc" << config_.GetSelfInfo().id()
  // << " primary id:" << message_manager_->GetCurrentPrimary();
  /*
  if(SignatureVerifier::CalculateHash(user_request->data()) !=
  user_request->hash()){ LOG(ERROR) << "the hash and data of the user request
  don't match, reject"; return -2;
  }
  */

  // check signatures
  bool valid = verifier_->VerifyMessage(user_request->data(),
                                        user_request->data_signature());
  if (!valid) {
    LOG(ERROR) << "request is not valid:"
               << user_request->data_signature().DebugString();
    LOG(ERROR) << " msg:" << user_request->data().size();
    return -2;
  }
  // Run custom verificatin if provided
  if (pre_verify_func_ && !pre_verify_func_(*user_request)) {
    LOG(ERROR) << " check by the user func fail";
    return -2;
  }

  // Track statistics
  global_stats_->IncClientRequest();

  // Check for duplicate proposal
  if (duplicate_manager_->CheckAndAddProposed(user_request->hash())) {
    return -2;
  }
  // Assign sequence number to the request
  auto seq = message_manager_->AssignNextSeq();

  // Artificially make the primary stop proposing new trasactions.

  if (!seq.ok()) {
    // Cleanup if sequence assignment fails
    duplicate_manager_->EraseProposed(user_request->hash());
    global_stats_->SeqFail();
    Request request;
    request.set_type(Request::TYPE_RESPONSE);
    request.set_sender_id(config_.GetSelfInfo().id());
    request.set_proxy_id(user_request->proxy_id());
    request.set_ret(-2);
    request.set_hash(user_request->hash());

    replica_communicator_->SendMessage(request, request.proxy_id());
    return -2;
  }

  global_stats_->RecordStateTime("request");

  // Convert user request to pre-prepare message
  // New: Convert user request to vote request message
  // user_request->set_type(Request::TYPE_PRE_PREPARE);
  user_request->set_type(Request::TYPE_3PC_VOTE_REQUEST);

  user_request->set_current_view(message_manager_->GetCurrentView());
  user_request->set_seq(*seq);
  user_request->set_sender_id(config_.GetSelfInfo().id());
  user_request->set_primary_id(config_.GetSelfInfo().id());

  // Broadcast pre-prepare message to all replicas
  // replica_communicator_->BroadCast(*user_request);
  // send them to replica with index 0,4,8,12

  replica_communicator_->SendMessage(*user_request, 1);
  replica_communicator_->SendMessage(*user_request, 5);
  replica_communicator_->SendMessage(*user_request, 9);
  replica_communicator_->SendMessage(*user_request, 13);

  return 0;
}

int Commitment::Process3PCVoteRequestMsg(std::unique_ptr<Context> context,
                                         std::unique_ptr<Request> request) {
  // LOG(INFO) << "Inside Process3PCVoteRequestMsg";

  // Create a new prepare message based on received request.
  std::unique_ptr<Request> vote_yes_request = resdb::NewRequest(
      Request::TYPE_3PC_VOTE_YES, *request, config_.GetSelfInfo().id());
  // vote_yes_request->clear_data();

  // -----
  // Pretend to be of message type TYPE_PRE_PREPARE and add to message_manager.
  // purpose is to change the state from TransactionStatue::None to
  // TransactionStatue::READY_PREPARE

  // request->set_type(Request::TYPE_PRE_PREPARE);

  // CollectorResultCode ret =
  // message_manager_->AddConsensusMsg(context->signature, std::move(request));
  // if (ret == CollectorResultCode::STATE_CHANGED) {
  // replica_communicator_->BroadCast(*vote_yes_request);

  // replica_communicator_->SendMessage(*vote_yes_request,
  //                                    message_manager_->GetCurrentPrimary());
  replica_communicator_->SendMessage(*vote_yes_request, request->primary_id());

  // } else {
  //   LOG(ERROR) << "consensus not reached inside Process3PCVoteRequestMsg";
  // }

  return 0;
}

int Commitment::Process3PCVoteYesMsg(std::unique_ptr<Context> context,
                                     std::unique_ptr<Request> request) {
  // LOG(INFO) << "Inside Process3PCVoteYesMsg";
  std::unique_ptr<Request> pre_commit_request = resdb::NewRequest(
      Request::TYPE_3PC_PRE_COMMIT, *request, config_.GetSelfInfo().id());
  // pre_commit_request->clear_data();
  CollectorResultCode ret =
      message_manager_->AddConsensusMsg(context->signature, std::move(request));

  if (ret == CollectorResultCode::STATE_CHANGED) {
    // replica_communicator_->BroadCast(*pre_commit_request);
    replica_communicator_->SendMessage(*pre_commit_request, 1);
    replica_communicator_->SendMessage(*pre_commit_request, 5);
    replica_communicator_->SendMessage(*pre_commit_request, 9);
    replica_communicator_->SendMessage(*pre_commit_request, 13);
  } else {
    // LOG(ERROR) << "consensus not reached inside Process3PCVoteYesMsg";
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

int Commitment::Process3PCPreCommitMsg(std::unique_ptr<Context> context,
                                       std::unique_ptr<Request> request) {
  std::unique_ptr<Request> precommit_ack_request = resdb::NewRequest(
      Request::TYPE_3PC_PRE_COMMIT_ACK, *request, config_.GetSelfInfo().id());
  // precommit_ack_request->clear_data();

  // replica_communicator_->SendMessage(*precommit_ack_request,
  //  message_manager_->GetCurrentPrimary());
  replica_communicator_->SendMessage(*precommit_ack_request,
                                     request->primary_id());

  return 0;
}

int Commitment::Process3PCPreCommitAckMsg(std::unique_ptr<Context> context,
                                          std::unique_ptr<Request> request) {
  // LOG(INFO) << "Inside Process3PCPreCommitAckMsg";
  std::unique_ptr<Request> commit_request = resdb::NewRequest(
      Request::TYPE_3PC_COMMIT, *request, config_.GetSelfInfo().id());
  // commit_request->clear_data();

  CollectorResultCode ret =
      message_manager_->AddConsensusMsg(context->signature, std::move(request));

  if (ret == CollectorResultCode::STATE_CHANGED) {
    // replica_communicator_->BroadCast(*commit_request);
    replica_communicator_->SendMessage(*commit_request, 1);
    replica_communicator_->SendMessage(*commit_request, 5);
    replica_communicator_->SendMessage(*commit_request, 9);
    replica_communicator_->SendMessage(*commit_request, 13);
  } else {
    // LOG(ERROR) << "consensus not reached inside Process3PCPreCommitAckMsg";
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

int Commitment::Process3PCCommitMsg(std::unique_ptr<Context> context,
                                    std::unique_ptr<Request> request) {
  // message_manager_->AddConsensusMsg(context->signature, std::move(request));

  // return 0;

  std::unique_ptr<Request> commit_ack_request = resdb::NewRequest(
      Request::TYPE_3PC_COMMIT_ACK, *request, config_.GetSelfInfo().id());
  // commit_ack_request->clear_data();

  // replica_communicator_->SendMessage(*commit_ack_request,
  //  message_manager_->GetCurrentPrimary());

  replica_communicator_->SendMessage(*commit_ack_request,
                                     request->primary_id());

  return 0;
}

int Commitment::Process3PCCommitAckMsg(std::unique_ptr<Context> context,
                                       std::unique_ptr<Request> request) {
  // LOG(INFO) << "Inside Process3PCCommitAckMsg";

  std::unique_ptr<Request> start_poe_request = resdb::NewRequest(
      Request::TYPE_START_POE, *request, config_.GetSelfInfo().id());

  replica_communicator_->SendMessage(*start_poe_request, 1);
  replica_communicator_->SendMessage(*start_poe_request, 5);
  replica_communicator_->SendMessage(*start_poe_request, 9);
  replica_communicator_->SendMessage(*start_poe_request, 13);

  return 0;
}

int Commitment::ProcessStartPOEMsg(std::unique_ptr<Context> context,
                                   std::unique_ptr<Request> request) {
  // LOG(INFO) << "Inside ProcessStartPOEMsg";
  LOG(INFO) << "Completed 3PC with Primary " << request->primary_id();

  std::unique_ptr<Request> pre_prepare_request = resdb::NewRequest(
      Request::TYPE_PRE_PREPARE, *request, config_.GetSelfInfo().id());
  // pre_prepare_request->clear_data();
  replica_communicator_->BroadCastWithinShard(*pre_prepare_request,
                                              config_.GetSelfInfo().id());

  return 0;
}

// Receive the pre-prepare message from the primary.
// TODO check whether the sender is the primary.
int Commitment::ProcessProposeMsg(std::unique_ptr<Context> context,
                                  std::unique_ptr<Request> request) {
  // LOG(INFO) << "Inside ProcessProposeMsg";

  // Check if node is in faulty state or if the request lacks proper
  // context/signature.
  if (global_stats_->IsFaulty() || context == nullptr ||
      context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject";
    return -2;
  }

  // Special handling for recovery requests, updating sequence number if
  // needed.
  if (request->is_recovery()) {
    if (message_manager_->GetNextSeq() == 0 ||
        request->seq() == message_manager_->GetNextSeq()) {
      message_manager_->SetNextSeq(request->seq() + 1);
    } else {
      LOG(ERROR) << " recovery request not valid:"
                 << " current seq:" << message_manager_->GetNextSeq()
                 << " data seq:" << request->seq();
      return 0;
    }
    return message_manager_->AddConsensusMsg(context->signature,
                                             std::move(request));
  }
  // Verify the request is from the primary.
  // if (request->sender_id() != message_manager_->GetCurrentPrimary()) {
  if (request->sender_id() !=
      message_manager_->GetCurrentShardPrimary(config_.GetSelfInfo().id())) {
    LOG(ERROR) << "the request is not from primary. sender:"
               << request->sender_id() << " seq:" << request->seq();
    return -2;
  }

  /*
    if(request->hash() != "null" + std::to_string(request->seq())
        && SignatureVerifier::CalculateHash(request->data()) !=
    request->hash()) { LOG(ERROR) << "the hash and data of the request don't
    match, reject"; return -2;
    }
    */

  // Additional validations for requestts from other nodes.
  if (request->sender_id() != config_.GetSelfInfo().id()) {
    if (pre_verify_func_ && !pre_verify_func_(*request)) {
      LOG(ERROR) << " check by the user func fail";
      return -2;
    }
    // global_stats_->GetTransactionDetails(std::move(request));
    BatchUserRequest batch_request;
    batch_request.ParseFromString(request->data());
    batch_request.clear_createtime();
    std::string data;
    batch_request.SerializeToString(&data);
    // check signatures
    bool valid =
        verifier_->VerifyMessage(request->data(), request->data_signature());
    if (!valid) {
      LOG(ERROR) << "request is not valid:"
                 << request->data_signature().DebugString();
      LOG(ERROR) << " msg:" << request->data().size();
      return -2;
    }
    if (duplicate_manager_->CheckAndAddProposed(request->hash())) {
      LOG(INFO) << "The request is already proposed, reject";
      return -2;
    }
  }

  // Increase the propose count and record the time for the pre-prepare state.
  global_stats_->IncPropose();
  global_stats_->RecordStateTime("pre-prepare");

  // Create a new prepare message based on received request.
  std::unique_ptr<Request> prepare_request = resdb::NewRequest(
      Request::TYPE_PREPARE, *request, config_.GetSelfInfo().id());
  prepare_request->clear_data();

  // Add request to message_manager.
  // If it has received enough same requests(2f+1), broadcast the prepare
  // message.
  // Asim: doesnt actually require 2f+1 messages to be received.
  // purpose is to add request to main_request smart pointer and
  // change the state from TransactionStatue::None to
  // TransactionStatue::READY_PREPARE
  CollectorResultCode ret =
      message_manager_->AddConsensusMsg(context->signature, std::move(request));
  if (ret == CollectorResultCode::STATE_CHANGED) {
    replica_communicator_->BroadCast(*prepare_request);
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
  // return 0;
}

// If receive 2f+1 prepare message, broadcast a commit message.
int Commitment::ProcessPrepareMsg(std::unique_ptr<Context> context,
                                  std::unique_ptr<Request> request) {
  // LOG(INFO) << "Inside ProcessPrepareMsg";

  if (context == nullptr || context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject";
    return -2;
  }
  if (request->is_recovery()) {
    return message_manager_->AddConsensusMsg(context->signature,
                                             std::move(request));
  }
  // global_stats_->IncPrepare();
  std::unique_ptr<Request> commit_request = resdb::NewRequest(
      Request::TYPE_COMMIT, *request, config_.GetSelfInfo().id());
  commit_request->mutable_data_signature()->Clear();
  // Add request to message_manager.
  // If it has received enough same requests(2f+1), broadcast the commit
  // message.
  uint64_t seq_ = request->seq();
  CollectorResultCode ret =
      message_manager_->AddConsensusMsg(context->signature, std::move(request));
  if (ret == CollectorResultCode::STATE_CHANGED) {
    if (message_manager_->GetHighestPreparedSeq() < seq_) {
      message_manager_->SetHighestPreparedSeq(seq_);
    }
    // If need qc, sign the data
    if (need_qc_ && verifier_) {
      auto signature_or = verifier_->SignMessage(commit_request->hash());
      if (!signature_or.ok()) {
        LOG(ERROR) << "Sign message fail";
        return -2;
      }
      *commit_request->mutable_data_signature() = *signature_or;
      // LOG(ERROR) << "sign hash"
      //           << commit_request->data_signature().DebugString();
    }
    global_stats_->RecordStateTime("prepare");
    // replica_communicator_->BroadCastWithinShard(*commit_request,
    // config_.GetSelfInfo().id());

    // replica_communicator_->BroadCast(*commit_request);
  } else {
    // LOG(ERROR) << "consensus not reached inside ProcessPrepareMsg";
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

// If receive 2f+1 commit message, commit the request.
int Commitment::ProcessCommitMsg(std::unique_ptr<Context> context,
                                 std::unique_ptr<Request> request) {
  // LOG(INFO) << "Inside ProcessCommitMsg";

  if (context == nullptr || context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject"
               << " context:" << (context == nullptr);
    return -2;
  }
  if (request->is_recovery()) {
    return message_manager_->AddConsensusMsg(context->signature,
                                             std::move(request));
  }
  // global_stats_->IncCommit();
  //  Add request to message_manager.
  //  If it has received enough same requests(2f+1), message manager will
  //  commit the request.
  CollectorResultCode ret =
      message_manager_->AddConsensusMsg(context->signature, std::move(request));
  if (ret == CollectorResultCode::STATE_CHANGED) {
    // LOG(ERROR)<<request->data().size();
    // global_stats_->GetTransactionDetails(request->data());
    global_stats_->RecordStateTime("commit");
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

// =========== private threads ===========================
// If the transaction is executed, send back to the proxy.
int Commitment::PostProcessExecutedMsg() {
  while (!stop_) {
    auto batch_resp = message_manager_->GetResponseMsg();
    if (batch_resp == nullptr) {
      continue;
    }
    global_stats_->SendSummary();
    Request request;
    request.set_hash(batch_resp->hash());
    request.set_seq(batch_resp->seq());
    request.set_type(Request::TYPE_RESPONSE);
    // // check if the id is 1,2,3,4
    // if (config_.GetSelfInfo().id() == 1 || config_.GetSelfInfo().id() == 2 ||
    //     config_.GetSelfInfo().id() == 3 || config_.GetSelfInfo().id() == 4) {
    // } else {
    //   request 0;
    // }
    request.set_sender_id(config_.GetSelfInfo().id());
    request.set_current_view(batch_resp->current_view());
    request.set_proxy_id(batch_resp->proxy_id());
    request.set_primary_id(batch_resp->primary_id());
    // LOG(ERROR)<<"send back to proxy:"<<batch_resp->proxy_id();
    batch_resp->SerializeToString(request.mutable_data());
    replica_communicator_->SendMessage(request, request.proxy_id());
  }
  return 0;
}

DuplicateManager* Commitment::GetDuplicateManager() {
  return duplicate_manager_.get();
}

}  // namespace resdb
