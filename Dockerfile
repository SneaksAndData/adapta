FROM python:3.9-bullseye

ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_HOME=/poetry
RUN curl -sSL https://install.python-poetry.org | python3 - --preview
COPY poetry.lock pyproject.toml ./
ENV PATH "/root/.local/bin:$PATH"
RUN /poetry/bin/poetry install
COPY . .
