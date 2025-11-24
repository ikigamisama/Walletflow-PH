FROM python:3.12-slim

WORKDIR /app


RUN pip install streamlit pandas plotly psycopg2-binary kafka-python

# Copy dashboard code
COPY app.py .

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# Run Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]