1. Architecture Overview

Frontend: Streamlit
Backend: FastAPI (REST Architecture)
Database: PostgreSQL
AI Layer: Gemini (Query Parsing)

The frontend communicates with the backend using HTTP requests.
The backend validates inputs, processes business logic, interacts with the database, and returns structured JSON responses.

The backend follows REST principles.

2. REST Design Principles Used

Resource-based endpoints
Proper HTTP methods (GET, POST, DELETE)
Stateless authentication (JWT)
Structured JSON responses
Proper HTTP status codes
Input validation before business logic (Pydantic)

3. API Endpoints
Authentication

POST /auth/register
Creates a new user account.

Request Body:

{
  "username": "string (min 3 chars)",
  "email": "string",
  "password": "string (min 6 chars)"
}

Responses:

200 → User registered successfully
400 → Username or email already exists

POST /auth/login
Authenticates user and returns JWT token.

Form Data:
username
password

Responses:
200 → Returns access_token
401 → Invalid credentials

Screener
POST /screener
Description:
Accepts natural language query. Parses it using Gemini AI. Applies filters to database.

Request Body:

{
  "query": "string (min 5 chars)"
}

Validation:
query must be at least 5 characters
Response:

{
  "status": "success",
  "data": [ ... stock results ... ]
}

Errors:

401 → Unauthorized
500 → AI parsing/database failure

Portfolio
POST /portfolio
Adds stock to user's portfolio.
Request Body:

{
  "stock_symbol": "string (1-5 chars)",
  "quantity": "integer > 0"
}

Validation:
stock_symbol required
quantity must be positive integer

Errors:
404 → Symbol not found
401 → Unauthorized

GET /portfolio
Returns all portfolio entries for logged-in user.
Response:

{
  "status": "success",
  "data": [
    {
      "id": 1,
      "symbol": "AAPL",
      "quantity": 10
    }
  ]
}

DELETE /portfolio/{portfolio_id}
Deletes a specific portfolio entry by ID.
Errors:

404 → Portfolio entry not found
401 → Unauthorized

Alerts
POST /alerts
Creates screening alert.
Request Body:
{
  "stock_symbol": "string",
  "metric": "pe_ratio | eps",
  "condition": "< | >",
  "threshold": "positive number"
}

Validation:

metric restricted to allowed values
condition restricted to allowed values
threshold must be > 0

Errors:
404 → Symbol not found
401 → Unauthorized

GET /alerts
Returns all alerts for logged-in user.
DELETE /alerts/{alert_id}
Deletes alert by ID.

Errors:

404 → Alert not found
401 → Unauthorized

4. HTTP Status Codes Used

200 → Success
400 → Bad Request (validation failure)
401 → Unauthorized (invalid token)
404 → Resource not found
500 → Internal Server Error

5. Security

JWT-based authentication
Password hashing using bcrypt
Protected routes using dependency injection
No direct frontend-to-database access

6. Data Reliability Considerations
Snapshot data stored in database
Delayed API data handled gracefully
Missing values handled using nullable filters
Query history logged
Background scheduler evaluates alerts periodically