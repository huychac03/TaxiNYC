from prefect import flow, task

@task
def say_hello():
	print("Hello, World! I'm Marvin!")


@flow(name="Prefect 2.0 Flow")
def marvin_flow():
	say_hello()


marvin_flow() # "Hello, World! I'm Marvin!"