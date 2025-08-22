import asyncio
from sickle import Sickle
import time, sys, os
from datetime import date, timedelta
import functools, urllib, socket
import logging

from sqlalchemy import create_engine, Column, Integer, Text, ARRAY, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
# do this:
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

# Connect to the database
DATABASE_URL = os.environ["DATABASE_URL"]
TABLE_NAME = os.environ["TABLE_NAME"]
engine = create_engine(DATABASE_URL)
Base = declarative_base()



class ZenodoRecord(Base):
    __tablename__ = TABLE_NAME

    creator = Column(Text)
    date = Column(Text)
    description = Column(Text)
    identifier = Column(Integer, primary_key=True, index=True)
    publisher = Column(Text)
    relation = Column(Text)
    rights = Column(Text) 
    subject = Column(Text)
    type = Column(Text)
    title = Column(Text)
    language = Column(Text)
    source = Column(Text)
    contributor = Column(Text)

Base.metadata.create_all(engine)

# Function written by @batukav
def retry_with_exponential_backoff(max_attempts=3, delay_seconds=1):
    """
    Decorator that retries a function with exponential backoff for transient network errors.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay_seconds
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)

                # --- New logic to handle non-retriable HTTP client errors ---
                except urllib.error.HTTPError as e:
                    # Check if the error is a client error (4xx) which is not likely to be resolved by a retry.
                    if 400 <= e.code < 500:
                        logger.error(
                            f"Function {func.__name__} failed with non-retriable client error {e.code}: {e.reason}"
                        )
                        raise e  # Re-raise the HTTPError immediately

                    # For other errors (like 5xx server errors), proceed with retry logic.
                    logger.warning(f"Caught retriable HTTPError {e.code}. Proceeding with retry...")
                    # Fall through to the generic exception handling below.

                except (urllib.error.URLError, socket.timeout) as e:
                    # This block now primarily handles non-HTTP errors or retriable HTTP errors.
                    pass # Fall through to the retry logic below

                # --- Existing retry logic ---
                attempts += 1
                if attempts < max_attempts:
                    logger.warning(
                        f"Attempt {attempts}/{max_attempts} for {func.__name__} failed. "
                        f"Retrying in {current_delay:.1f} seconds..."
                    )
                    time.sleep(current_delay)
                    current_delay *= 2
                else:
                    logger.error(
                        f"Function {func.__name__} failed after {max_attempts} attempts."
                    )
                    # Re-raise the last exception caught to be handled by the caller
                    raise ConnectionError(
                        f"Function {func.__name__} failed after {max_attempts} attempts."
                    )
        return wrapper
    return decorator


def generate_dates(start_date, end_date):
    # Generate dates in daily steps
    current_date = start_date
    daily_dates = []
    while current_date <= end_date:
        daily_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    return daily_dates


def to_string(array):
    return ";".join(array)


def add_record(for_db, session):
    record = session.query(ZenodoRecord).filter_by(identifier=for_db['identifier']).first()

    if record:
        # Update existing record
        for key, value in for_db.items():
            setattr(record, key, value)
    else:
        # Insert new record
        record = ZenodoRecord(**for_db)
        session.add(record)
    session.commit()
        

async def process_metadata(record, session):
    param_set = {"creator", "identifier", "title",
            "description",
            "publisher",
            "relation",
            "rights",
            "language",
            "subject",
            "type",
            "date",
            "source", "contributor"}
    identifier = int(record.header.identifier.split(":")[-1])

    md = record.metadata
    
    record = {k: to_string(v) for k, v in md.items()}
    record["identifier"] = identifier
    missing = param_set - set(md.keys())
    if missing != set():
        missing = list(missing)
        for el in missing:
            record[el] = ""

# YOU STOPPED HERE!!!
        await asyncio.to_thread(add_record, record, session)
        # print("added")
        return record
    else:
        await asyncio.to_thread(add_record, record, session)
        # print("added")
        return record
    

@retry_with_exponential_backoff(max_attempts=5, delay_seconds=2)
def get_next_record(records):
    try:
        result = next(records)
    except Exception as e:
        print(e)
        print("######################################")
        return None
    return result


async def producer(q, records):
    record = 1
    while True:
        record = await asyncio.to_thread(get_next_record, records)
        if record is None:
            break
        await q.put(record)
        await asyncio.sleep(0.01)
        # print(".", end="")
        sys.stdout.flush()
    
    await q.join()
    await q.put(None)  # poison pill
    print("Poison pill sent!")


async def consumer(q, session):
    print("Start async processing")
    while True:
        item = await q.get()
        if item is None:
            print("x")
            q.task_done()
            break

        md_to_db = await process_metadata(item, session)
        
        # print("!", end="")
        await asyncio.sleep(0.05)
        sys.stdout.flush()
        q.task_done()
        
    print("Consumer exit")
        

async def main():

    sickle = Sickle('https://zenodo.org/oai2d')

    # Period to harvest
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 31)
    dates = generate_dates(start_date, end_date)
    SessionLocal = sessionmaker(bind=engine)

    for i in range(len(dates)-1):
        print(dates[i], dates[i+1])
        records = sickle.ListRecords(**{'metadataPrefix': 'oai_dc',
                                     'from': dates[i],
                                     'until': dates[i+1]})
        
        print(f"Fetched {start_date} - {end_date}")

        # CONNECTION POSTGRES
        session = SessionLocal()

        q = asyncio.Queue()
        prod = asyncio.create_task(producer(q, records))
        cons = asyncio.create_task(consumer(q, session))
        await prod
        await cons
        session.commit()
        session.close()
        print("Added day")
        

asyncio.run(main())