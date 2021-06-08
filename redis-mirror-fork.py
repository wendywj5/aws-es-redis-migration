import click
import sys
import redis
from time import sleep
import re
import threading


@click.command()
@click.option("--shost", type=str, default="127.0.0.1", help="Source redis host/IP.")
@click.option("--sport", type=int, default=6379, help="Source redis port.")
@click.option("--sdb", type=int, default="0", required=False, help="Source redis DB.")
@click.option(
    "--sauth", default=None, type=str, required=False, help="Source redis auth info."
)
@click.option(
    "--dhost", type=str, default="127.0.0.1", help="Destination redis host/IP."
)
@click.option("--dport", type=int, default=6377, help="Destination redis port.")
@click.option(
    "--ddb", type=int, default="0", required=False, help="Destination redis DB."
)
@click.option(
    "--dauth",
    default=None,
    type=str,
    required=False,
    help="Destination redis auth info.",
)
@click.option("--limit", type=int, help="Stop mirror process at limit X.")
@click.option(
    "--replace", type=bool, is_flag=True, default=False, help="Replace key if exists."
)
def main(shost, sport, sdb, sauth, dhost, dport, ddb, dauth, limit, replace):
    """The main function
    Args:
        shost (str): source redis host
        sport (int): source redis port
        sdb (int): source redis database number
        sauth (str): source redis auth info
        dhost (str): destination redis host
        dport (int): destination redis port
        ddb (int): destination redis database number
        dauth (str): destination redis auth info
        limit (int): number of iterations to stop script on it
        replace (bool): replace key if exists
    """
    s = makeConnection(shost, sport, sdb, sauth)
    d = makeConnection(dhost, dport, ddb, dauth)
    getSTDOUT(s, d, limit, replace)


def makeConnection(host, port, db, auth):
    """Function to create redis connection
    Note:
        If the connection failed, the program will exit 1
    Args:
        host (str): redis connection string
        port (int): redis port number
        db (int): redis database name
        auth (str): auth info for redis
    Returns:
        connection: connection object for redis
    """
    if auth:
        pool = redis.ConnectionPool(host=host, port=port, db=db, password=auth)
    else:
        pool = redis.ConnectionPool(host=host, port=port, db=db)
    r = redis.StrictRedis(connection_pool=pool)
    try:
        r.ping()
        print(f"Redis is connected, Host; {host}, Port:{port}, DB:{db}")
    except Exception as e:
        print(f"Redis connection error ({e})")
        sys.exit(1)
    return r


def split(delimiters, data, maxsplit=0):
    """Split input redis monitor data stram and get the key name
    Args:
        delimiters str: [description]
        data str: [description]
        maxsplit (int, optional): max split length. Defaults to 0.
    Returns:
        str: redis key name
    """
    try:
        regexPattern = "|".join(map(re.escape, delimiters))
        data = re.split(regexPattern, data, maxsplit)
        data = data[3]
        return data
    except Exception as e:
        return None

def split_value(delimiters, data, maxsplit=0):
    try:
        regexPattern = "|".join(map(re.escape, delimiters))
        data = re.split(regexPattern, data, maxsplit)
        data = data[5]
        return data
    except Exception as e:
        return None

def stdinStream():
    """Get STDIN
    Returns:
        _io.TextIOWrapper: STDIN stream
    """
    if not sys.stdin.isatty():
        input_stream = sys.stdin
    else:
        print("There is no stdin, check help for more info. exit 1")
        sys.exit(1)
    return input_stream


def getSTDOUT(sourceConnection, destinationConnection, limit, replace):
    """Read STDIN
       dump from source redis
       restore key to destination redis
    Args:
        sourceConnection (connection): redis source connection object
        destinationConnection (connection): redis destination connection object
        limit (int): number to stop mirror process
        replace (bool): replace key if exists in the destination redis
    """
    counter = 0
    for line in stdinStream():
        key = split('"', line, 5)
        # if not key == None and '] "DUMP" "' not in line:
        #     counter = counter + 1
        #     data = sourceConnection.dump(key)
        #     try:
        #         destinationConnection.restore(key, 0, data, replace=replace)
        #         print(f"Mirrored key | {key}")
        #     except Exception as e:
        #         print(f"Skip {key} becouse of  {e}")
        #     if counter == limit:
        #         print(f"Limit reached {counter}")
        #         sys.exit(0)
        if '] "select" "' in line:
            continue
        if not key == None and '] "EXPIRE" "' in line:
            value = split_value('"', line, 10)
            print("value" + value)
            destinationConnection.expire(key,int(value))
        if not key == None and '] "DEL" "' in line:
            value = split_value('"', line, 10)
            print("value" + value)
            destinationConnection.delete(key)
        if not key == None and '] "DUMP" "' not in line and '] "TTL" "' not in line:
            counter = counter + 1
            data = sourceConnection.dump(key)
            try:
                destinationConnection.restore(key, 0, data, replace=replace)
                print(f"Mirrored key | {key}")  
                ttl = sourceConnection.ttl(key)
                if ttl > 0 : 
                    destinationConnection.expire(key,ttl)
                    print(f"Mirrored ttl | {str(ttl)}")
            except Exception as e:
                print(f"Skip because of  {e}") 

if __name__ == "__main__":
    main()
