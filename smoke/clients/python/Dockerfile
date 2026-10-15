FROM python:3.13-slim
ARG ZB_VERSION
RUN pip install --no-cache-dir "zebridge==${ZB_VERSION}"
COPY smoke.py /smoke.py
CMD ["python", "/smoke.py"]
