/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/client/types.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/produce.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/interface.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>
#include <fmt/ostream.h>

#include <tuple>

static ss::logger fpt_logger("fpt_test");

using namespace std::chrono_literals; // NOLINT

struct produce_partition_fixture : redpanda_thread_fixture {
    static constexpr size_t topic_name_length = 30;
    static constexpr size_t total_partition_count = 1;
    static constexpr size_t session_partition_count = 100;

    model::topic t;

    produce_partition_fixture() {
        BOOST_TEST_CHECKPOINT("before leadership");

        wait_for_controller_leadership().get0();

        BOOST_TEST_CHECKPOINT("HERE");

        t = model::topic(
          random_generators::gen_alphanum_string(topic_name_length));
        auto tp = model::topic_partition(t, model::partition_id(0));
        add_topic(
          model::topic_namespace_view(model::kafka_namespace, t),
          total_partition_count)
          .get();

        auto ntp = make_default_ntp(tp.topic, tp.partition);
        wait_for_leader(ntp).get0();
        BOOST_TEST_CHECKPOINT("HERE");
    }
};

PERF_TEST_C(produce_partition_fixture, test_produce_partition) {
    // make the fetch topic
    //    kafka::fetch_topic ft;
    //    ft.name = t;

    //    // add the partitions to the fetch request
    //    for (int pid = 0; pid < session_partition_count; pid++) {
    //        kafka::fetch_partition fp;
    //        fp.partition_index = model::partition_id(pid);
    //        fp.fetch_offset = model::offset(0);
    //        fp.current_leader_epoch = kafka::leader_epoch(-1);
    //        fp.log_start_offset = model::offset(-1);
    //        fp.max_bytes = 1048576;
    //        ft.fetch_partitions.push_back(std::move(fp));
    //    }

    BOOST_TEST_CHECKPOINT("HERE");

    // model::record_batch batch;

    model::topic_partition tp = model::topic_partition(
      t, model::partition_id(0));

    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    for (size_t i = 0; i < 1; ++i) {
        builder.add_raw_kv(iobuf{}, iobuf{});
    }

    auto batch = std::move(builder).build();

    // TODO(mfleming) Hack. This shouldn't be necessary but avoids a vassert()
    // in segment_appender.cc. See storage::disk_header_to_iobuf().
    // batch.header().size_bytes = model::packed_record_batch_header_size;

    std::vector<kafka::produce_request::partition> partitions;
    partitions.push_back(kafka::produce_request::partition{
      .partition_index{model::partition_id(0)},
      .records = kafka::produce_request_record_data(std::move(batch))});

    std::vector<kafka::produce_request::topic> topics;
    topics.push_back(kafka::produce_request::topic{
      .name{std::move(tp.topic)}, .partitions{std::move(partitions)}});

    std::optional<ss::sstring> t_id;
    auto acks = -1;
    kafka::produce_request produce_req = kafka::produce_request(
      t_id, acks, std::move(topics));

    BOOST_TEST_CHECKPOINT("HERE");

    // we need to share a connection among any requests here since the
    // session cache is associated with a connection
    auto conn = make_connection_context();

    BOOST_TEST_CHECKPOINT("HERE");

    kafka::request_header header{
      .key = kafka::produce_handler::api::key,
      .version = kafka::produce_handler::max_supported};

    // Use a fake_req to generate the context because encoding the request
    // steals the request's iobufs.
    kafka::produce_request fake_req;
    auto rctx = make_request_context(fake_req, header, conn);

    // add all partitions to fetch metadata
    //    auto& mdc = rctx.get_fetch_metadata_cache();
    //    for (int i = 0; i < total_partition_count; i++) {
    //        mdc.insert_or_assign(
    //          {t, i}, model::offset(0), model::offset(100),
    //          model::offset(100));
    //    }

    //    vassert(mdc.size() == total_partition_count, "mdc.size(): {}",
    //    mdc.size());

    //    auto octx = kafka::op_context(
    //      std::move(rctx), ss::default_smp_service_group());
    //
    //    BOOST_REQUIRE(!octx.session_ctx.is_sessionless());
    //    BOOST_REQUIRE_EQUAL(octx.session_ctx.session()->id(), sess_id);

    BOOST_TEST_CHECKPOINT("HERE");

    constexpr size_t iters = 1; // 10000; // 0000000U;

    kafka::produce_ctx pctx{
      std::move(rctx),
      std::move(produce_req),
      kafka::produce_response{},
      ss::default_smp_service_group()};

    std::vector<ss::future<>> dispatched;
    std::vector<ss::future<kafka::produce_response::partition>> produced;
    perf_tests::start_measuring_time();
    for (size_t i = 0; i < iters; i++) {
        vassert(
          pctx.request.data.topics.size() == 1,
          "topics.size(): {}",
          pctx.request.data.topics.size());
        auto& topic = pctx.request.data.topics.front();
        vassert(
          topic.partitions.size() == 1,
          "partitions.size(): {}",
          topic.partitions.size());
        auto& partition = topic.partitions.front();

        auto stages = kafka::testing::produce_single_partition(
          pctx, topic, partition);
        perf_tests::do_not_optimize(stages);

        dispatched.push_back(std::move(stages.dispatched));
        produced.push_back(std::move(stages.produced));
    }
    perf_tests::stop_measuring_time();

    co_await ss::when_all_succeed(dispatched.begin(), dispatched.end())
      .then_wrapped([produced = std::move(produced)](ss::future<> f) mutable {
          try {
              f.get();
              return when_all_succeed(produced.begin(), produced.end())
                .then(
                  [&](std::vector<kafka::produce_response::partition> partitions) {
                      for (const auto& p : partitions) {
                          vassert(
                            p.error_code == kafka::error_code::none,
                            "error_code: {}",
                            p.error_code);
                      }
                  });
          } catch (...) {
              vassert(false, "exception: {}", std::current_exception());
          }
      });
}
