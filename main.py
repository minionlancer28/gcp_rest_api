import json

# noinspection PyPackageRequirements
from google.cloud import bigtable
# noinspection PyPackageRequirements
from google.cloud import datastore
# noinspection PyPackageRequirements
from google.cloud.bigtable import row_filters
# noinspection PyPackageRequirements
from google.cloud.bigtable.row_filters import RowFilterChain, ValueRangeFilter, \
    PassAllFilter, ColumnRangeFilter
# noinspection PyPackageRequirements
from google.cloud.bigtable.row_set import RowSet

PROJECT_ID = 'digitaleyes-prod'
BIGTABLE_INSTANCE_ID = "digitaleyes-prod-instance"
BIGTABLE_TRANSACTIONS_TABLE_ID = 'transactions'
DATASTORE_OFFER_KIND = 'Offer'

PAGE_SIZE = 20

datastore_client = datastore.Client(project=PROJECT_ID)
bigtable_client = bigtable.Client(project=PROJECT_ID, admin=True)

instance = bigtable_client.instance(BIGTABLE_INSTANCE_ID)
transactions_table = instance.table(BIGTABLE_TRANSACTIONS_TABLE_ID)


def run(request):
    request_args = request.args

    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return '', 204, headers

    sort_by = []
    sortable = ['price', 'addEpoch']
    for item in sortable:
        if item in request_args:
            if request_args[item] == 'desc':
                sort_by.append(f"-{item}")
            else:
                sort_by.append(item)

    if 'collection' in request_args:
        collection = request_args['collection']
        if 'cursor' in request_args:
            cursor = request_args['cursor']
        else:
            cursor = None

        filter_keys = [key for key in request_args.keys()
                       if key != 'collection' and key != 'cursor' and key != 'price' and key != 'addEpoch']
        filters = [{'filter_name': key, 'filter_value': request_args[key]} for key in filter_keys]

        return _process_request(collection, cursor, sort_by, filters), 200, {'Content-Type': 'application/json',
                                                                             'Access-Control-Allow-Origin': '*'}
    elif 'mint' in request_args and 'owner' in request_args:
        mint = request_args['mint']
        owner = request_args['owner']
        return {"offers": _process_request_for_single_mint_with_owner(mint, owner)}, 200, {'Content-Type': 'application/json',
                                                             'Access-Control-Allow-Origin': '*'}
    elif 'pk' in request_args:
        pk = request_args['pk']
        return _process_request_for_pk(pk), 200, {'Content-Type': 'application/json',
                                                             'Access-Control-Allow-Origin': '*'}
    elif 'mint' in request_args:
        mint = request_args['mint']
        return _process_request_for_single_mint(mint), 200, {'Content-Type': 'application/json',
                                                             'Access-Control-Allow-Origin': '*'}
    else:
        # returns unverified items
        if 'cursor' in request_args:
            cursor = request_args['cursor']
        else:
            cursor = None

        return _process_request(None, cursor, sort_by, []), 200, {'Content-Type': 'application/json',
                                                                  'Access-Control-Allow-Origin': '*'}


def _process_request_for_pk(pk):
    offer_key = datastore_client.key(
        DATASTORE_OFFER_KIND, pk
    )
    offer_entity = datastore_client.get(offer_key)
    if offer_entity is None:
        return {}
    else:
        offer_json = json.loads(json.dumps(offer_entity))
        offer_json['pk'] = offer_entity.key.name
        return offer_json


def _process_request_for_single_mint(mint):
    query = datastore_client.query(kind=DATASTORE_OFFER_KIND)
    query.add_filter('mint', '=', mint)

    offers, _ = _get_one_page(query, None)
    if len(offers) > 0:
        offer_json = json.loads(json.dumps(offers[0]))
        offer_json['pk'] = offers[0].key.name
        return offer_json
    else:
        return {}


def _process_request_for_single_mint_with_owner(mint, owner):
    query = datastore_client.query(kind=DATASTORE_OFFER_KIND)
    query.add_filter('mint', '=', mint)
    query.add_filter('owner', '=', owner)
    offer_entity_list = list(query.fetch())
    offer_json_list = []
    for offer in offer_entity_list:
        offer_json = json.loads(json.dumps(offer))
        offer_json['pk'] = offer.key.name
        offer_json_list.append(offer_json)
    return offer_json_list


def _process_request(collection, cursor, sort_by, filters):
    offer_query = _create_base_offer_query(collection, filters)
    offer_query.order = sort_by

    offers, next_cursor = _get_one_page(offer_query, cursor)
    mints = [e['mint'] for e in offers]

    last_prices_dict = {}
    if len(offers) > 0:
        if collection is not None:
            last_prices_dict = _get_last_prices_for_mints(collection, mints)
        else:
            last_prices_dict = _get_last_prices_for_mints('Unverifeyed', mints)

    processed_offers = [_process_offer(e, last_prices_dict) for e in offers]

    result = {
        'offers': [],
        'next_cursor': None,
        'price_floor': None,
        'count': None
    }

    if len(processed_offers) > 0:
        result['offers'] = processed_offers
        result['next_cursor'] = next_cursor
        if cursor is None:
            # compute the price floor and the count for the first page, not for all
            price_query = _create_base_offer_query(collection, filters)
            price_query.order = ['price']
            found_price = list(price_query.fetch(limit=1))[0]["price"]  # guaranteed to exist
            result['price_floor'] = found_price

            offer_count_query = _create_base_offer_query(collection, filters)
            offer_count_query.keys_only()
            offer_count = len(list(offer_count_query.fetch()))
            result['count'] = offer_count

    return result


def _create_base_offer_query(collection, filters):
    base_query = datastore_client.query(kind=DATASTORE_OFFER_KIND)
    if collection is not None:
        base_query.add_filter('collection', '=', collection)
    else:
        base_query.add_filter('verifeyed', '=', False)
    for f in filters:
        base_query.add_filter('tags', '=', f"{f['filter_name']}={f['filter_value']}")
    return base_query


# noinspection PyDefaultArgument
def _get_one_page(query, required_cursor=None):
    """
    Given a cursor, it returns a page of PAGE_SIZE offers from Datastore.
    If the cursor is None, it starts from the beginning.

    The cursor should change whenever the query changes.
      i.e. if a new filter is applied, the new cursor should be None.
    """
    query_iter = query.fetch(start_cursor=required_cursor, limit=PAGE_SIZE)
    page = next(query_iter.pages)

    offers = list(page)
    next_cursor = query_iter.next_page_token

    if next_cursor is None:
        return offers, None
    else:
        return offers, next_cursor.decode('utf8')


def _process_offer(offer, last_prices_dict):
    """
    Converts a Datastore Entity object into a JSON object, to prepare it for JSON serialization in the HTTP response.

    We also enhance the offer body with a few more details.
    """
    offer_json = json.loads(json.dumps(offer))
    offer_json['pk'] = offer.key.name
    if offer_json['mint'] in last_prices_dict:
        offer_json['lastPrice'] = last_prices_dict[offer_json['mint']]

    return offer_json


def _get_last_prices_for_mints(required_collection, required_mints):
    """
    This performs a Bigtable query to retrieve the last price of each mint, if it exists.

    The way is to create multiple row ranges for each provided mint.

    :param required_collection: The collection to be used to create the Bigtable row keys.
    :param required_mints: The list of mints for which the last price has to be returned.
    :return: A dictionary of the form {'mint': last_price}
    """
    row_set = RowSet()
    for m in required_mints:
        row_set.add_row_range_with_prefix(f'{required_collection}#{m}#')

    condition = RowFilterChain(  # Sends a row through several filters in sequence
        filters=[
            ColumnRangeFilter("metadata", b"type", b"type", inclusive_end=True),
            ValueRangeFilter(b'"SALE"')
        ]
    )
    # The ConditionalRowFilter executes one of two filters based on another filter.
    # If the base_filter returns any cells in the row, then true_filter is executed.
    # If not, then false_filter is executed.
    conditional_filter = row_filters.ConditionalRowFilter(
        base_filter=condition,
        true_filter=PassAllFilter(True),  # this way you get the whole row and not just the single cell value
    )

    rows = transactions_table.read_rows(
        row_set=row_set,
        filter_=conditional_filter
    )

    latest_prices = {}
    for row in rows:
        curr_mint = row.row_key.decode('utf-8').split('#')[1]
        if curr_mint not in latest_prices:
            # md will be of the form (column_family, columns)
            md = next(filter(lambda item: item[0] == 'metadata', row.cells.items()), None)
            if md is not None:
                latest_prices[curr_mint] = int(md[1][b'price'][0].value.decode('utf-8'))

    return latest_prices


if __name__ == '__main__':
    # todo: add args here?
    r = _process_request('Degenerate Ape Academy', None, '', [{'filter_name': 'sequence', 'filter_value': '6656'}])
    print(json.dumps(r, indent=4))
