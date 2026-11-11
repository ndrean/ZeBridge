"""The baseline must distinguish a settled predecessor from an older in-flight write."""
import tempfile, unittest
from pathlib import Path
from ingress_baseline import trace_summary

class TraceTests(unittest.TestCase):
    def test_immediate_predecessor_and_older_write_are_distinct(self):
        lines='''IO_BATCH D 10 1 1
IO_MSG D 10 1 b0w0 1
IO_BATCH D 20 2 1
IO_MSG D 20 2 b0w1 1
IO_BATCH C 30 2 1
IO_MSG C 30 2 b0w1 1
IO_BATCH D 40 2 2
IO_MSG D 40 2 b0w2 1
IO_MSG D 40 2 b0w3 1
IO_BATCH C 50 1 1
IO_MSG C 50 1 b0w0 1
IO_BATCH C 60 2 2
IO_MSG C 60 2 b0w2 1
IO_MSG C 60 2 b0w3 1
'''
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'trace';path.write_text(lines)
            got=trace_summary(path)
        self.assertEqual(got['predecessor_states'],dict(no_predecessor=1,dispatched_elsewhere_commit_not_yet_observed=1,commit_observed_before_dispatch=1,same_batch=1))
        self.assertEqual(got['any_earlier_write_in_other_batch_commit_unobserved'],3)
        self.assertEqual(got['unique_dispatches'],4)
        self.assertEqual(got['commits'],4)
        self.assertEqual(got['batch_sizes'],{1:2,2:1})

    def test_same_batch_is_not_necessarily_in_dependency_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'trace'
            path.write_text('IO_BATCH D 10 1 2\nIO_MSG D 10 1 b0w1 1\nIO_MSG D 10 1 b0w0 1\n')
            got=trace_summary(path)
        self.assertEqual(got['predecessor_states']['same_batch_reversed'],1)

    def test_settled_at_creation_means_no_predecessor(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'trace'
            path.write_text('IO_BATCH D 10 1 1\nIO_MSG D 10 1 b0w0 1\nIO_MSG C 20 1 b0w0 1\nIO_BATCH D 30 1 1\nIO_MSG D 30 1 b0w1 1\n')
            got=trace_summary(path, {'b0w0':None, 'b0w1':None})
        self.assertEqual(got['predecessor_states'], {'no_predecessor':2})

    def test_structured_benchmark_id_maps_to_the_same_writer(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'trace'
            mid='zb1-t123-insert-b0-ib_rows-k1-b0w0-version'
            path.write_text(f'IO_BATCH D 10 1 1\nIO_MSG D 10 1 {mid} 1\nIO_MSG C 20 1 {mid} 1\n')
            got=trace_summary(path)
        self.assertEqual(got['predecessor_states'], {'no_predecessor':1})
        self.assertEqual(got['commits'],1)

if __name__=='__main__':unittest.main()
