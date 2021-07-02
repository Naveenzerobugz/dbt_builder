import mock, json
from dbt_builder import TransformGenerator


def test_get_filter_query():
    with open('filter.json') as f:
        filter_data = json.load(f)
    generator = TransformGenerator()
    stmt = generator.generate_for_model(filter_data)
    return stmt


def test_get_agg_query():
    with open('agg.json') as f:
        agg_data = json.load(f)
    generator = TransformGenerator()
    stmt = generator.generate_for_model(agg_data)
    return stmt


@mock.patch('transforms.main.do_request')
def test_get_all_queries_flow(mock_request):
    with open('flow_1.json') as f:
        flow_data = json.load(f)
    mock_request.return_value = flow_data
    # setup(70)


print(test_get_agg_query())
# print(test_get_filter_query())
# print(test_get_all_queries_flow())
