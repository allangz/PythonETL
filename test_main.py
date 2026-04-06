from main import possible_lead
import config


class TestPossibleLeadFunction:
    def test_possible_lead_high(self):
        row = {'age': config.HIGH_AGE_LIMIT, 'Number of employees': config.HIGH_HEADCOUNT_LIMIT}
        assert possible_lead(row) == 'High'

    def test_possible_lead_medium(self):
        row = {'age': config.MEDIUM_AGE_LIMIT, 'Number of employees': config.MEDIUM_HEADCOUNT_LIMIT}
        assert possible_lead(row) == 'Medium'

    def test_possible_lead_low(self):
        row = {'age': config.AGE_LIMIT, 'Number of employees': config.EMPLOYEES_LIMIT}
        assert possible_lead(row) == 'Low'
