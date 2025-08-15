FROM ghcr.io/astral-sh/uv:0.8.14 AS uv
FROM public.ecr.aws/lambda/python:3.12-arm64 AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_INSTALLER_METADATA=1
ENV UV_LINK_MODE=copy

COPY uv.lock .
COPY pyproject.toml .
COPY --from=uv /uv /bin/uv
RUN uv export --group ml --frozen --no-emit-workspace --no-dev --no-editable > requirements.txt && \
    uv pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

FROM public.ecr.aws/lambda/python:3.12-arm64
COPY --from=builder ${LAMBDA_TASK_ROOT} ${LAMBDA_TASK_ROOT}
COPY . ./

ENV PYTHONPATH=.
CMD ["lambda.functions.crawler.lambda_handler"]

