import ujson


def get_db_description(path='./db_configs.json'):
    with open(path) as f:
        db_description = ujson.load(f)
    return db_description
