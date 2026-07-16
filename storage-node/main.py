from storage_node.main import app


if __name__ == "__main__":
	import uvicorn

	uvicorn.run("storage_node.main:app", host="127.0.0.1", port=8001, reload=False)

