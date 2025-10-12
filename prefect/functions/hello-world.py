from prefect import flow, task
import random

@task(log_prints=True)
def task_one():
    print("Executing Task One")
    fruits = ['Apple', 'Grapes', 'Pear', 'Strawberry']
    return random.choice(fruits)

@task(log_prints=True)
def task_two(fruit):
    print("Executing Task Two")
    print("==================")
    print(f"The randomly selected fruit was: {fruit}")

@flow(log_prints=True)
def simple_flow():
    fruit = task_one()
    task_two(fruit)

if __name__ == "__main__":
    simple_flow()
