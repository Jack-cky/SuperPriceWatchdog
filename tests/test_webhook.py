import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from superpricewatchdog import create_app


def test_index_page():
    app = create_app()

    with app.test_client() as test_client:
        response = test_client.get("/")

        assert response.status_code == 200
