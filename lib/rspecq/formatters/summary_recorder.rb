module RSpecQ
  module Formatters
    # Keeps the job's summary so that the worker can persist its example
    # count and timing together with the job acknowledgement. Timings are
    # used when determining the ordering in which jobs are scheduled (slower
    # jobs will be enqueued first).
    class SummaryRecorder
      attr_reader :summary

      def dump_summary(summary)
        @summary = summary
      end
    end
  end
end
