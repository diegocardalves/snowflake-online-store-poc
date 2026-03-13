# Stage 1: install all deps in a full Python image that has gcc/cmake/etc.
FROM python:3.11 AS builder

WORKDIR /build
COPY requirements.txt .
# Force manylinux2014 wheels (GLIBC >= 2.17) so packages run on Amazon Linux 2.
# --only-binary=:all: prevents source builds that would compile against Debian's newer GLIBC.
RUN pip install \
    --no-cache-dir \
    --platform manylinux2014_x86_64 \
    --python-version 3.11 \
    --implementation cp \
    --only-binary=:all: \
    --target /build/packages \
    -r requirements.txt

# Stage 2: copy only the installed packages into the Lambda runtime image
FROM public.ecr.aws/lambda/python:3.11

COPY --from=builder /build/packages ${LAMBDA_TASK_ROOT}
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

CMD ["lambda_function.handler"]
