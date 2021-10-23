# from pipeline.steps.SPECIFIC_DETAILS import SPECIFIC_DETAILS
# from pipeline.steps import crm_redeem, crm_point, select
from pipeline.steps import *
from pipeline.steps.step import StepException


from pipeline.pipeline import Pipeline
from utils import Utils

CARD_NO = '74000234401|74000132545'
CRM_POINT_ENDDATE = '1017'
DATA_PERIOD = 'wk42'
STORE = '491|209|53'


def main():
    inputs = {
        'card_no': CARD_NO,
        'end_date': CRM_POINT_ENDDATE,
        'date_period': DATA_PERIOD,
        'storeid': STORE,
    }
    steps = [
        # SPECIFIC_DETAILS.SPECIFIC_DETAILS(),
        # crm_redeem.CRM_REDEEM(),
        # crm_point.CRM_POINT(),
        # insufficient_point.Insufficient_Point(),
        exchange.Exchange(),
        # select.Select_Store(),
        # select.Select_CardNo(),
    ]
    utils = Utils()
    p = Pipeline(steps)
    p.run(inputs, utils)


if __name__ == '__main__':
    main()