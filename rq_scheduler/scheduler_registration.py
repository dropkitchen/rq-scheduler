SCHEDULERS_BY_QUEUE_KEY = 'rq:schedulers:%s'
REDIS_SCHEDULER_KEYS = 'rq:schedulers'


def register(scheduler, pipeline=None):
    """Store scheduler key in Redis so we can easily discover active scheduler"""
    connection = pipeline if pipeline is not None else scheduler.connection
    connection.sadd(scheduler.redis_schedulers_keys, scheduler.key)

    redis_key = SCHEDULERS_BY_QUEUE_KEY % scheduler.queue_name
    connection.sadd(redis_key, scheduler.key)


def unregister(scheduler, pipeline=None):
    """Remove scheduler key from Redis"""
    if pipeline is None:
        connection = scheduler.connection.pipeline()
    else:
        connection = pipeline

    connection.srem(scheduler.redis_schedulers_keys, scheduler.key)
    redis_key = SCHEDULERS_BY_QUEUE_KEY % scheduler.queue_name
    connection.srem(redis_key, scheduler.key)

    if pipeline is None:
        connection.execute()


def get_keys(queue=None, connection=None):
    """Returns a list of scheduler keys for a queue"""
    if queue is None and connection is None:
        raise ValueError('"queue" or "connection" argument is required')

    if queue:
        redis = queue.connection
        redis_key = SCHEDULERS_BY_QUEUE_KEY % queue.name
    else:
        redis = connection
        redis_key = REDIS_SCHEDULER_KEYS

    return {key for key in redis.smembers(redis_key)}
