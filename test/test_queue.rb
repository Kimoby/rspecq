require "test_helpers"

class TestQueue < RSpecQTest
  def test_flaky_jobs
    build_id = rand_id

    pid, queue = start_worker("flaky_job_detection", build_id: build_id)
    Process.wait(pid)

    assert_queue_well_formed(queue)
    refute queue.build_successful?

    assert_equal ["./spec/flaky_spec.rb[1:1]", "./spec/flaky_spec.rb[1:3]"],
      queue.flaky_jobs.sort

    assert_processed_jobs(
      ["./spec/passing_spec.rb",
       "./spec/flaky_spec.rb",
       "./spec/flaky_spec.rb[1:1]",
       "./spec/flaky_spec.rb[1:3]",
       "./spec/legit_failure_spec.rb",
       "./spec/legit_failure_spec.rb[1:3]"], queue
    )

    assert_equal 3, queue.flaky_failures.size # legit failure + 2 flakes

    assert_equal [
      "./spec/flaky_spec.rb[1:1]",
      "./spec/flaky_spec.rb[1:3]",
      "./spec/legit_failure_spec.rb[1:3]"
      ],
      queue.flaky_failures.keys.sort

    assert_failures(["./spec/legit_failure_spec.rb[1:3]"], queue)
  end

  def test_push_jobs_returns_jobs_size
    queue = RSpecQ::Queue.new(rand_id, rand_id, REDIS_OPTS)

    assert_equal 2, queue.push_jobs(["job1", "job2"])
  end

  def test_timings_reconstruction
    queue = RSpecQ::Queue.new(rand_id, rand_id, REDIS_OPTS)

    queue.record_build_timing("foo", 42.0)
    queue.record_build_timing("foo[1]", 1.0)

    queue.update_global_timings

    global_timings = queue.global_timings

    assert_equal 42.0, global_timings["foo"], "File timing should not be overriden"
  end

  def test_default_timing
    queue = RSpecQ::Queue.new(rand_id, rand_id, REDIS_OPTS)

    # Record file timings
    queue.record_build_timing("foo", 100.0)
    queue.record_build_timing("bar", 200.0)
    queue.record_build_timing("baz", 300.0)

    # Record individual example timings
    queue.record_build_timing("foo[1]", 1.0)
    queue.record_build_timing("foo[2]", 2.0)
    queue.record_build_timing("bar[1]", 3.0)
    queue.record_build_timing("bar[2]", 4.0)
    queue.record_build_timing("baz[1]", 5.0)
    queue.record_build_timing("baz[2]", 6.0)

    queue.update_global_timings

    worker = new_worker("not-existing")
    assert_equal 200, worker.default_timing, "Default timing should be the median of file timings, not example timings"
  end

  def test_reserve_next_job_reserves_and_records_heartbeat
    queue = RSpecQ::Queue.new(rand_id, rand_id, REDIS_OPTS)
    queue.push_jobs(["job1", "job2"])

    assert_equal ["reserved", "job1", nil, nil], queue.reserve_next_job
    assert_equal ["job2"], queue.unprocessed_jobs
    assert_includes queue.worker_heartbeats.keys, queue.worker_id
  end

  def test_reserve_next_job_is_empty_until_published
    queue = RSpecQ::Queue.new(rand_id, rand_id, REDIS_OPTS)
    queue.push_jobs(["job1"], publish: false)
    queue.redis.lpop(queue.key_queue_unprocessed)

    assert_equal "empty", queue.reserve_next_job.first
  end

  def test_reserve_next_job_is_empty_while_others_still_run
    build_id = rand_id
    other = RSpecQ::Queue.new(build_id, rand_id, REDIS_OPTS)
    queue = RSpecQ::Queue.new(build_id, rand_id, REDIS_OPTS)
    queue.push_jobs(["job1"])

    assert_equal "reserved", other.reserve_next_job.first
    assert_equal "empty", queue.reserve_next_job.first

    other.complete_job("job1", example_count: 1, duration: 1.0)
    assert_equal "exhausted", queue.reserve_next_job.first
  end

  def test_reserve_next_job_requeues_the_job_of_a_dead_worker
    build_id = rand_id
    dead = RSpecQ::Queue.new(build_id, "dead", REDIS_OPTS)
    queue = RSpecQ::Queue.new(build_id, rand_id, REDIS_OPTS)
    queue.push_jobs(["job1", "job2"])

    assert_equal "job1", dead.reserve_next_job[1]
    dead.redis.zadd(dead.key_worker_heartbeats, 0, "dead")

    assert_equal ["reserved", "job1", "job1", "dead"], queue.reserve_next_job
    assert_equal 1, queue.lost_jobs_count
    assert_equal ["job2"], queue.unprocessed_jobs
  end

  def test_reserve_next_job_stops_when_build_failed_fast
    queue = RSpecQ::Queue.new(rand_id, rand_id, REDIS_OPTS)
    queue.push_jobs(["job1"], 1)
    queue.record_example_failure("job0[1:1]", "boom")

    assert_equal ["failed_fast"], queue.reserve_next_job
    assert_equal ["job1"], queue.unprocessed_jobs
  end

  def test_complete_job
    queue = RSpecQ::Queue.new(rand_id, rand_id, REDIS_OPTS)
    queue.push_jobs(["job1"])
    queue.reserve_next_job

    queue.complete_job("job1", example_count: 3, duration: 1.5)

    assert_equal 3, queue.example_count
    assert_equal 1.5, queue.job_build_timing("job1")
    assert_equal 1500, queue.total_execution_time_ms
    assert_equal ["job1"], queue.processed_jobs
    assert_equal "exhausted", queue.reserve_next_job.first
  end

  def test_complete_job_without_summary_only_acknowledges
    queue = RSpecQ::Queue.new(rand_id, rand_id, REDIS_OPTS)
    queue.push_jobs(["job1"])
    queue.reserve_next_job

    queue.complete_job("job1", example_count: nil, duration: nil)

    assert_equal 0, queue.example_count
    assert_nil queue.job_build_timing("job1")
    assert_equal ["job1"], queue.processed_jobs
  end
end
