"""known_red's exit 1 must mean exactly the known defect: any other failure is 3, a missing run 2."""
import copy, unittest
import ingress_arrival as A
import ingress_ordering as O

OK = {'status': 'accepted', 'sqlstate': ''}
FK = {'status': 'rejected', 'sqlstate': '23503'}
CONTROL = dict(lanes=1, passed=True, parent_verdict=OK, child_verdict=OK, parent_rows=1, child_rows=1)
PARALLEL = dict(lanes=4, passed=False, child_executed_while_parent_held=True, parent_verdict=OK, parent_rows=1,
                child_verdict=FK, child_rows=0, unrelated_verdict_while_parent_held=OK, unrelated_rows_while_parent_held=1)
PROBE = dict(case='rate-limit', passed=True, parent_verdict=OK, child_verdict=OK, parent_rows=1, child_rows=1)
REFUSED = dict(case='admission', passed=False, parent_admission_refusal={'code': 503}, parent_verdict=OK, parent_rows=1,
               child_verdict=FK, child_rows=0)


def changed(row, **kw):
    r = copy.deepcopy(row)
    for k, v in kw.items():
        if isinstance(v, dict): r[k] = {**r[k], **v}
        else: r[k] = v
    return r


class Ordering(unittest.TestCase):
    def test_exact_signature_is_1(self):
        self.assertEqual(O.verdict_code([CONTROL, PARALLEL], 2), 1)

    def test_any_other_failure_is_3(self):
        for rows in ([changed(CONTROL, passed=False), PARALLEL],
                     [CONTROL, changed(PARALLEL, child_verdict={'sqlstate': '23514'})],
                     [CONTROL, changed(PARALLEL, parent_verdict={'status': 'rejected'})],
                     [CONTROL, changed(PARALLEL, unrelated_rows_while_parent_held=0)],
                     [CONTROL, changed(PARALLEL, child_executed_while_parent_held=False)],
                     [PARALLEL, PARALLEL]):  # no one-lane control at all
            self.assertEqual(O.verdict_code(rows, len(rows)), 3, rows)

    def test_harness_trouble_is_2_and_fixed_is_0(self):
        self.assertEqual(O.verdict_code([CONTROL], 2), 2)
        self.assertEqual(O.verdict_code([CONTROL, changed(PARALLEL, precondition_error='x')], 2), 2)
        self.assertEqual(O.verdict_code([CONTROL, changed(PARALLEL, passed=True)], 2), 0)


class Arrival(unittest.TestCase):
    def test_exact_signature_is_1(self):
        self.assertEqual(A.verdict_code([REFUSED, PROBE] * 3, 6), 1)

    def test_any_other_failure_is_3(self):
        for rows in ([REFUSED, changed(PROBE, passed=False)],
                     [changed(REFUSED, child_verdict={'sqlstate': '40001'}), PROBE],
                     [changed(REFUSED, parent_rows=0), PROBE],
                     [REFUSED, REFUSED]):  # no rate-limit probe at all
            self.assertEqual(A.verdict_code(rows, len(rows)), 3, rows)

    def test_harness_trouble_is_2_and_fixed_is_0(self):
        self.assertEqual(A.verdict_code([REFUSED, PROBE] * 2 + [REFUSED], 6), 2)
        self.assertEqual(A.verdict_code([changed(REFUSED, passed=True), PROBE], 2), 0)


if __name__ == '__main__':
    unittest.main()
