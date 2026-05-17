from prefect import flow, task
import random

@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(100), k=10)]

@task
def process_customer_item(customer_id: str, id: int) -> str:
    # Process a single customer
    return f"Processed {customer_id}, item {id}"

@task
def process_customer(customer_id: str) -> str:
    # Process a single customer
    i1 = process_customer_item(customer_id, 1)
    i2 = process_customer_item(customer_id, 2)
    return f"Processed {customer_id}: {i1} and {i2}"

@flow
def main() -> list[str]:
    customer_ids = get_customer_ids()
    # Map the process_customer task across all customer IDs
    results = []
    for i in customer_ids:
        results.append(process_customer(i))
    print(f"Results: {results}")
    return results


if __name__ == "__main__":
    main()
