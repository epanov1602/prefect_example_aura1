from prefect import flow, task
import random

@task
def get_customer_ids() -> list[int]:
    # Fetch customer IDs from a database or API
    return [n for n in random.choices(range(100), k=10)]

@task
def process_customer_item(customer_id: int, item_id: int) -> str:
    # Process a single customer
    import time
    time.sleep(5)
    #assert item_id != 1
    return f"Processed {customer_id}, item {item_id}"

@task
def process_customer(customer_id: int) -> str:
    # Process a single customer
    future_items = process_customer_item.map(customer_id=customer_id, item_id=range(1 + (customer_id % 3)))
    items = [i.result() for i in future_items]
    return f"Processed {customer_id}: {items}"

@flow
def main() -> list[str]:
    customer_ids = get_customer_ids()
    # Map the process_customer task across all customer IDs
    results = process_customer.map(customer_ids)
    printable = [r.result() for r in results]
    print(f"Results: {printable}")
    return printable

if __name__ == "__main__":
    main()
