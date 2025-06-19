import os
import base64
import json
import datetime
import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import firebase_admin
from firebase_admin import firestore, credentials
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PORT = int(os.getenv("PORT", 8000))

app = FastAPI(
    title="M-Pesa STK Push API",
    description="FastAPI implementation for M-Pesa STK Push integration",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Firebase Admin initialization
def initialize_firebase():
    if not firebase_admin._apps:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        cred_path = os.path.join(BASE_DIR, "kassmatt-f28c7-firebase-adminsdk-jmtlo-0bb2879fc1.json")


        if cred_path and os.path.exists(cred_path):
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred)
        else:
            # Use default credentials (for production)
            firebase_admin.initialize_app()
    return firestore.client()

db = initialize_firebase()

# Configuration from environment variables
CONSUMER_KEY = os.getenv("MPESA_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET")
BUSINESS_SHORT_CODE = os.getenv("MPESA_BUSINESS_SHORT_CODE", "5501736")
PASS_KEY = os.getenv("MPESA_PASS_KEY")
CALLBACK_URL = os.getenv("MPESA_CALLBACK_URL")
TRANSACTION_TYPE = "CustomerBuyGoodsOnline"
ACCOUNT_REFERENCE = "CompanyXYZ"
TRANSACTION_DESC = "Payment for services"
PARTY_B = "4986750"

# API endpoints
BASE_URL = os.getenv("MPESA_BASE_URL", "https://api.safaricom.co.ke")
AUTH_URL = f"{BASE_URL}/oauth/v1/generate?grant_type=client_credentials"
STK_PUSH_URL = f"{BASE_URL}/mpesa/stkpush/v1/processrequest"
QUERY_URL = f"{BASE_URL}/mpesa/stkpushquery/v1/query"

# Pydantic models
class STKPushRequest(BaseModel):
    phone_number: str
    amount: float
    account_reference: Optional[str] = ACCOUNT_REFERENCE
    transaction_desc: Optional[str] = TRANSACTION_DESC

class TransactionStatusRequest(BaseModel):
    checkout_request_id: str

class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None

def get_access_token() -> str:
    """Get OAuth access token from M-Pesa"""
    if not CONSUMER_KEY or not CONSUMER_SECRET:
        raise HTTPException(
            status_code=500, 
            detail="M-Pesa credentials not configured"
        )
    
    auth_string = f"{CONSUMER_KEY}:{CONSUMER_SECRET}"
    b64_auth = base64.b64encode(auth_string.encode()).decode('utf-8')

    headers = {
        "Authorization": f"Basic {b64_auth}"
    }

    try:
        response = requests.get(AUTH_URL, headers=headers)
        response.raise_for_status()
        access_token = response.json().get('access_token')
        if not access_token:
            raise HTTPException(
                status_code=500, 
                detail="Failed to get access token from M-Pesa"
            )
        return access_token
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error getting access token: {str(e)}"
        )

def generate_password() -> tuple[str, str]:
    """Generate the LipaNaMpesa Online password"""
    if not PASS_KEY:
        raise HTTPException(
            status_code=500, 
            detail="M-Pesa Pass Key not configured"
        )
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    password_str = f"{BUSINESS_SHORT_CODE}{PASS_KEY}{timestamp}"
    password = base64.b64encode(password_str.encode()).decode('utf-8')
    return password, timestamp

def format_phone_number(phone_number: str) -> str:
    """Ensure phone number is in the correct international format"""
    # Remove any spaces or special characters
    phone_number = ''.join(filter(str.isdigit, phone_number))
    
    if phone_number.startswith('0'):
        phone_number = '254' + phone_number[1:]
    elif not phone_number.startswith('254'):
        phone_number = '254' + phone_number
    return phone_number

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "M-Pesa STK Push API"}

@app.post("/initiate-stk-push", response_model=APIResponse)
async def initiate_stk_push(request: STKPushRequest):
    """Initiate M-Pesa STK Push payment"""
    try:
        # Format and validate phone number
        phone_number = format_phone_number(request.phone_number)
        amount = int(float(request.amount))  # Ensure it's an integer

        # Validate phone number format
        if not (len(phone_number) == 12 and phone_number.isdigit()):
            raise HTTPException(
                status_code=400,
                detail="Invalid phone number format. Use format: 0722000000 or 254722000000"
            )

        if amount <= 0:
            raise HTTPException(
                status_code=400,
                detail="Amount must be greater than 0"
            )

        # Generate password and timestamp
        password, timestamp = generate_password()

        # Get access token
        access_token = get_access_token()

        # Prepare STK Push request
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "BusinessShortCode": BUSINESS_SHORT_CODE,
            "Password": password,
            "Timestamp": timestamp,
            "TransactionType": TRANSACTION_TYPE,
            "Amount": amount,
            "PartyA": phone_number,
            "PartyB": PARTY_B,
            "PhoneNumber": phone_number,
            "CallBackURL": CALLBACK_URL,
            "AccountReference": request.account_reference,
            "TransactionDesc": request.transaction_desc
        }

        # Make the STK Push request
        response = requests.post(STK_PUSH_URL, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()

        # Check if request was successful
        if result.get('ResponseCode') == '0':
            # Store transaction details in Firestore
            checkout_id = result.get('CheckoutRequestID')
            transaction_data = {
                'phone_number': phone_number,
                'amount': amount,
                'timestamp': firestore.SERVER_TIMESTAMP,
                'checkout_request_id': checkout_id,
                'status': 'pending',
                'merchant_request_id': result.get('MerchantRequestID'),
                'account_reference': request.account_reference,
                'transaction_desc': request.transaction_desc
            }

            # Add transaction to Firestore
            db.collection('mpesa_transactions').document(checkout_id).set(transaction_data)

            return APIResponse(
                success=True,
                message="STK push request sent successfully",
                data={
                    "checkout_request_id": checkout_id,
                    "merchant_request_id": result.get('MerchantRequestID'),
                    "phone_number": phone_number,
                    "amount": amount
                }
            )

        raise HTTPException(
            status_code=400,
            detail=f"STK push request failed: {result.get('errorMessage', 'Unknown error')}"
        )

    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to communicate with M-Pesa API: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )

@app.post("/mpesa-callback")
async def mpesa_callback(request: Request):
    """Handle M-Pesa STK Push callbacks"""
    try:
        # Extract callback data
        callback_data = await request.json()
        print(f"Received M-Pesa callback: {json.dumps(callback_data)}")
        
        # Extract the Body section which contains the actual transaction data
        body = callback_data.get('Body', {})
        stkCallback = body.get('stkCallback', {})
        
        checkout_request_id = stkCallback.get('CheckoutRequestID')
        result_code = stkCallback.get('ResultCode')
        result_desc = stkCallback.get('ResultDesc')

        if not checkout_request_id:
            return {"ResultCode": 1, "ResultDesc": "Invalid callback data"}

        # Get transaction reference from Firestore
        transaction_ref = db.collection('mpesa_transactions').document(checkout_request_id)
        
        # Default transaction update data
        transaction_update = {
            'result_code': result_code,
            'result_description': result_desc,
            'callback_received': True,
            'callback_timestamp': firestore.SERVER_TIMESTAMP,
            'raw_callback': callback_data
        }
        
        # Check if the transaction was successful
        if result_code == 0:  # Successful transaction
            transaction_update['status'] = 'completed'
            
            # Extract additional details if available
            if 'CallbackMetadata' in stkCallback:
                items = stkCallback['CallbackMetadata'].get('Item', [])
                
                # Extract specific fields from callback metadata
                for item in items:
                    if item['Name'] == 'Amount':
                        # Ensure amount is stored as float
                        transaction_update['confirmed_amount'] = float(item['Value'])
                    elif item['Name'] == 'MpesaReceiptNumber':
                        # Receipt number should be string
                        transaction_update['mpesa_receipt_number'] = str(item['Value'])
                    elif item['Name'] == 'TransactionDate':
                        # Store transaction date as string or convert to proper format
                        transaction_update['transaction_date'] = str(item['Value'])
                    elif item['Name'] == 'PhoneNumber':
                        # FIX: Convert phone number to string
                        phone_value = item['Value']
                        if isinstance(phone_value, (int, float)):
                            # Convert integer/float to string
                            transaction_update['confirmed_phone_number'] = str(int(phone_value))
                        else:
                            transaction_update['confirmed_phone_number'] = str(phone_value)
        else:
            # Transaction failed - map common result codes
            if result_code == 1032:
                transaction_update['status'] = 'cancelled'
            elif result_code == 1037:
                transaction_update['status'] = 'expired'
            elif result_code == 1:
                transaction_update['status'] = 'insufficient_funds'
            else:
                transaction_update['status'] = 'failed'
        
        # Update transaction in Firestore
        transaction_ref.update(transaction_update)
        
        # Return success response to M-Pesa
        return {
            "ResultCode": 0,
            "ResultDesc": "Callback processed successfully"
        }
    
    except Exception as e:
        print(f"Error processing M-Pesa callback: {str(e)}")
        # Log the full traceback for debugging
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            "ResultCode": 1,
            "ResultDesc": "Callback processing failed"
        }

# Also update the check_transaction_status function for consistency
@app.post("/check-transaction-status", response_model=APIResponse)
async def check_transaction_status(request: TransactionStatusRequest):
    """Check the status of an M-Pesa transaction"""
    try:
        checkout_request_id = request.checkout_request_id
        
        # First check if we have the result in Firestore
        transaction_doc = db.collection('mpesa_transactions').document(checkout_request_id).get()
        
        if not transaction_doc.exists:
            raise HTTPException(
                status_code=404,
                detail="Transaction not found"
            )
        
        transaction_data = transaction_doc.to_dict()
        
        # Check if callback has been received
        if transaction_data.get('callback_received', False):
            return APIResponse(
                success=True,
                message="Transaction status retrieved from database",
                data={
                    "checkout_request_id": checkout_request_id,
                    "status": transaction_data.get('status'),
                    "result_code": transaction_data.get('result_code'),
                    "result_description": transaction_data.get('result_description'),
                    "receipt_number": transaction_data.get('mpesa_receipt_number'),
                    "amount": transaction_data.get('confirmed_amount', transaction_data.get('amount')),
                    "phone_number": transaction_data.get('confirmed_phone_number', transaction_data.get('phone_number')),
                    "transaction_date": transaction_data.get('transaction_date')
                }
            )
        
        # If we haven't received a callback yet, query the API
        merchant_request_id = transaction_data.get('merchant_request_id')
        
        if not merchant_request_id:
            raise HTTPException(
                status_code=400,
                detail="Merchant request ID not found for this transaction"
            )
        
        # Get access token
        access_token = get_access_token()
        
        # Generate the password for the query
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        password_str = f"{BUSINESS_SHORT_CODE}{PASS_KEY}{timestamp}"
        password = base64.b64encode(password_str.encode()).decode('utf-8')
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "BusinessShortCode": BUSINESS_SHORT_CODE,
            "Password": password,
            "Timestamp": timestamp,
            "CheckoutRequestID": checkout_request_id
        }
        
        # Make the query request
        response = requests.post(QUERY_URL, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        # Update transaction status in Firestore based on query result
        result_code = result.get('ResultCode')
        result_desc = result.get('ResultDesc')
        
        status_update = {
            'query_result_code': result_code,
            'query_result_description': result_desc,
            'last_queried': firestore.SERVER_TIMESTAMP
        }
        
        # Determine transaction status based on result code with better mapping
        if result_code == 0:
            status_update['status'] = 'completed'
        elif result_code == 1032:  # Transaction cancelled by user
            status_update['status'] = 'cancelled'
        elif result_code == 1037:  # Timeout waiting for user input
            status_update['status'] = 'expired'
        elif result_code == 1031:  # User cancelled
            status_update['status'] = 'cancelled'
        elif result_code == 1:  # Insufficient funds
            status_update['status'] = 'insufficient_funds'
        else:
            status_update['status'] = 'failed'
        
        # Update transaction in Firestore
        db.collection('mpesa_transactions').document(checkout_request_id).update(status_update)
        
        return APIResponse(
            success=True,
            message="Transaction status retrieved from M-Pesa API",
            data={
                "checkout_request_id": checkout_request_id,
                "status": status_update['status'],
                "result_code": result_code,
                "result_description": result_desc,
                "amount": transaction_data.get('amount'),
                "phone_number": transaction_data.get('phone_number')
            }
        )
    
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to communicate with M-Pesa API: {str(e)}"
        )
    except Exception as e:
        # Better error logging
        import traceback
        print(f"Error in check_transaction_status: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )
@app.get("/transaction/{checkout_request_id}", response_model=APIResponse)
async def get_transaction(checkout_request_id: str):
    """Get transaction details by checkout request ID"""
    try:
        # Get transaction from Firestore
        transaction_doc = db.collection('mpesa_transactions').document(checkout_request_id).get()
        
        if not transaction_doc.exists:
            raise HTTPException(
                status_code=404,
                detail="Transaction not found"
            )
        
        transaction_data = transaction_doc.to_dict()
        
        # Convert Firestore timestamps to ISO format for JSON serialization
        if 'timestamp' in transaction_data and transaction_data['timestamp']:
            transaction_data['timestamp'] = transaction_data['timestamp'].isoformat()
        if 'callback_timestamp' in transaction_data and transaction_data['callback_timestamp']:
            transaction_data['callback_timestamp'] = transaction_data['callback_timestamp'].isoformat()
        if 'last_queried' in transaction_data and transaction_data['last_queried']:
            transaction_data['last_queried'] = transaction_data['last_queried'].isoformat()
        
        return APIResponse(
            success=True,
            message="Transaction retrieved successfully",
            data=transaction_data
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while retrieving the transaction: {str(e)}"
        )

@app.get("/transactions")
async def get_all_transactions(
    limit: int = 10,
    status: Optional[str] = None,
    phone_number: Optional[str] = None
):
    """Get all transactions with optional filtering"""
    try:
        query = db.collection('mpesa_transactions')
        
        # Apply filters
        if status:
            query = query.where('status', '==', status)
        if phone_number:
            formatted_phone = format_phone_number(phone_number)
            query = query.where('phone_number', '==', formatted_phone)
        
        # Order by timestamp and limit results
        query = query.order_by('timestamp', direction=firestore.Query.DESCENDING).limit(limit)
        
        docs = query.stream()
        transactions = []
        
        for doc in docs:
            transaction_data = doc.to_dict()
            transaction_data['id'] = doc.id
            
            # Convert timestamps
            if 'timestamp' in transaction_data and transaction_data['timestamp']:
                transaction_data['timestamp'] = transaction_data['timestamp'].isoformat()
            if 'callback_timestamp' in transaction_data and transaction_data['callback_timestamp']:
                transaction_data['callback_timestamp'] = transaction_data['callback_timestamp'].isoformat()
            if 'last_queried' in transaction_data and transaction_data['last_queried']:
                transaction_data['last_queried'] = transaction_data['last_queried'].isoformat()
            
            transactions.append(transaction_data)
        
        return APIResponse(
            success=True,
            message=f"Retrieved {len(transactions)} transactions",
            data={"transactions": transactions}
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while retrieving transactions: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)