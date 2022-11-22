FROM public.ecr.aws/lambda/python:3.9-x86_64
RUN /var/lang/bin/python -m pip install --upgrade pip
RUN python -m pip install marshmallow amazon-textract-response-parser==0.1.34  --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY app/ ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "main.lambda_handler" ]
