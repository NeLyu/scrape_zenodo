import asyncio
from sickle import Sickle
import time, sys, os
from datetime import date, timedelta
import functools, urllib, socket, requests
import logging

from sqlalchemy import create_engine, Column, Integer, Text, ARRAY, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select

from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv

from bs4 import BeautifulSoup
import html
import traceback

logger = logging.getLogger(__name__)
load_dotenv()

# Connect to the database
DATABASE_URL = os.environ["DATABASE_URL"]
TABLE_NAME = os.environ["TABLE_NAME"]
# engine = create_engine(DATABASE_URL)
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

# Base.metadata.create_all(engine)

# Function written by @batukav
def retry_with_exponential_backoff(max_attempts=500, delay_seconds=1):
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

                except StopIteration:
                    # Don’t retry if the iterator is exhausted
                    raise

                # --- New logic to handle non-retriable HTTP client errors ---
                except (requests.exceptions.HTTPError, urllib.error.HTTPError) as e:
                    status = None
                    # urllib
                    if hasattr(e, "status"):
                        status = e.status
                    # requests
                    if hasattr(e, "response") and e.response is not None:
                        status = e.response.status_code
                    
                    reason = getattr(e, "reason", "")

                    if status is None:
                        raise

                    # Check if the error is a client error (4xx) which is not likely to be resolved by a retry.
                    if 400 <= status < 500:
                        print(f"Function {func.__name__} failed with non-retriable client error {status}: {reason}")
                        # logger.error(
                        #     f"Function {func.__name__} failed with non-retriable client error {e.code}: {e.reason}"
                        # )
                        raise e  # Re-raise the HTTPError immediately

                    # For other errors (like 5xx server errors), proceed with retry logic.
                    logger.warning(f"Caught retriable HTTPError {status}. Proceeding with retry...")
                    # Fall through to the generic exception handling below.

                # Handle requests connection/timeouts
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        urllib.error.URLError,
                        socket.timeout) as e:
                        # logger.warning(f"{func.__name__} network error: {e}, retrying...")
                        print(f"{func.__name__} network error: {e}, retrying...")

                # --- Existing retry logic ---
                attempts += 1
                if attempts < max_attempts:
                    print(f"Attempt {attempts}/{max_attempts} for {func.__name__} failed. ")
                    print(f"Retrying in {current_delay:.1f} seconds...")
                    # logger.warning(
                    #     f"Attempt {attempts}/{max_attempts} for {func.__name__} failed. "
                    #     f"Retrying in {current_delay:.1f} seconds..."
                    # )
                    time.sleep(current_delay)
                    current_delay *= 2
                else:
                    # logger.error(
                    #     f"Function {func.__name__} failed after {max_attempts} attempts."
                    # )
                    print(f"Function {func.__name__} failed after {max_attempts} attempts.")
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
    return "; ".join(array)


async def add_record(for_db, async_session):
    try:
        async with async_session() as session:
            # async with session.begin():
                # print("try")
                # record = session.query(ZenodoRecord).filter_by(identifier=for_db['identifier']).first()
                record = insert(ZenodoRecord).values([for_db])
                # print("q")
                for_db_no_id = for_db.copy()
                for_db_no_id.pop("identifier")
                # If conflict on id → update name
                upsert_record = record.on_conflict_do_update(
                    index_elements=["identifier"],  # conflict target
                    set_=for_db_no_id,  # update if conflict
                )
                await session.execute(upsert_record)
                await session.commit()

    except Exception as e:
        print("Failed query")
        traceback.print_exc()


def clean_html(text):
    decoded = html.unescape(text)
    clean = BeautifulSoup(decoded, "html.parser").get_text()
    return clean
        

async def process_metadata(rcd, async_session):
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
    identifier = int(rcd.header.identifier.split(":")[-1])

    md = rcd.metadata
    record = {k: "; ".join(v) for k, v in md.items()}
    record["identifier"] = identifier
    missing = param_set - set(md.keys())
    if missing != set():
        missing = list(missing)
        for el in missing:
            record[el] = ""

    clean_description = await asyncio.to_thread(clean_html, record["description"])
    record["description"] = clean_description

    await add_record(record, async_session)
    # print("added")
    return record
    

@retry_with_exponential_backoff(max_attempts=500, delay_seconds=2)
def get_next_record(records):
    try:
        result = next(records)
        return result
    except StopIteration as e:
        print(e)
        print("######################################")
        return None
    except Exception as e:
        raise e
    


async def producer(q, records):
    record = 1
    while True:
        record = await asyncio.to_thread(get_next_record, records)
        if record is None:
            break
        await q.put(record)
        await asyncio.sleep(0.01)
        # print(".", end="")
        # sys.stdout.flush()
    
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
        # print("?")
        md_to_db = await process_metadata(item, session)
        
        # print("!", end="")
        await asyncio.sleep(0.05)
        sys.stdout.flush()
        q.task_done()
        
    print("Consumer exit")
        

async def main():

    sickle = Sickle('https://zenodo.org/oai2d')

    # Period to harvest
    start_date = date(2024, 8, 1)
    end_date = date(2024, 12, 31)
    dates = generate_dates(start_date, end_date)

    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


    for i in range(len(dates)-1):
        print(dates[i], dates[i+1])
        records = sickle.ListRecords(**{'metadataPrefix': 'oai_dc',
                                     'from': dates[i],
                                     'until': dates[i+1]})
        
        print(f"Fetched {dates[i]} - {dates[i+1]}")

        q = asyncio.Queue()
        prod = asyncio.create_task(producer(q, records))
        cons = asyncio.create_task(consumer(q, async_session))
        await prod
        await cons

        print("Added day")
        

asyncio.run(main())