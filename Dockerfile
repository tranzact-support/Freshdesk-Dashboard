FROM python:3.11-slim
WORKDIR /app
COPY . /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
EXPOSE 8787
CMD ["sh", "-c", "python3 freshdesk_activity_dashboard_web.py --host 0.0.0.0 --port ${PORT:-8787}"]
