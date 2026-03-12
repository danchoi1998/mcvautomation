#!/bin/bash
# Start the MCV Streamlit app for team access
# Usage: ./start_app.sh

cd "$(dirname "$0")"

# Kill any existing instance
pkill -f "streamlit run src/app.py" 2>/dev/null

echo "Starting MCV Automation app..."
echo "Team access URL: http://$(hostname -I | awk '{print $1}'):8501"
echo "Local URL:       http://localhost:8501"
echo ""
echo "Press Ctrl+C to stop, or close this terminal."
echo "To run in background instead: ./start_app.sh &"
echo ""

streamlit run src/app.py \
    --server.address 0.0.0.0 \
    --server.port 8501 \
    --server.headless true
