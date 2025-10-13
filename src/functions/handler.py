from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from db_operation import insert_or_update_users_bulk

app = FastAPI()

@app.post("/users")
async def main(request: Request):
    try:
        # Parse incoming JSON body
        json_array = await request.json()
        if not isinstance(json_array, list):
            raise HTTPException(status_code=400, detail="Payload must be a JSON array")

        # Filter users whose userID starts with 'P'
        valid_users = [user for user in json_array if str(user.get("userID", "")).startswith("P")]

        # Log skipped users
        skipped_users = [user for user in json_array if not str(user.get("userID", "")).startswith("P")]
        if skipped_users:
            print(f"Skipped users (invalid userID): {[user.get('userID') for user in skipped_users]}")

        # Call DB operation
        if valid_users:
            result = insert_or_update_users_bulk(valid_users)
            return JSONResponse(content={"status": "success", "processed": len(valid_users)}, status_code=200)
        else:
            return JSONResponse(content={"status": "no_valid_users", "processed": 0}, status_code=200)

    except Exception as e:
        # Log and return error
        print(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=str(e))
