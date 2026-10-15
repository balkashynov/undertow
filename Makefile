.PHONY: test serve demo lab
test:
	.venv/bin/python -m pytest -q
serve:
	.venv/bin/undertow serve
demo:
	.venv/bin/python scripts/build_demo.py
lab:
	.venv/bin/python scripts/lab.py --output docs/evidence/reproduction
