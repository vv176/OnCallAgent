import os
import time
import json
import base64
import uuid
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Request, HTTPException
import httpx
import openai
from twilio.rest import Client
from dotenv import load_dotenv
load_dotenv()

# Configuration
AGENT_PORT = int(os.getenv("AGENT_PORT", "8088"))
LB_URL = os.getenv("LB_URL", "http://127.0.0.1:9000")
PROM_URL = os.getenv("PROM_URL", "http://127.0.0.1:9090")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = "vv176"
GITHUB_REPO = "OnCallAgent"
GITHUB_REF = "main"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Twilio Configuration
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
USER_WHATSAPP_NUMBER = os.getenv("USER_WHATSAPP_NUMBER", "whatsapp:+919581176653")

# Jira Configuration
JIRA_URL = os.getenv("JIRA_URL")
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
JIRA_PROJECT_KEY = os.getenv("JIRA_PROJECT_KEY", "PROJ")

# Initialize clients
client = openai.OpenAI(api_key=OPENAI_API_KEY)
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN else None

# In-memory conversation storage (single conversation for now)
conversation_storage = {}

app = FastAPI(title="OnCallAgent")

# System prompt for the LLM. Refine it to handle more incidents.
SYSTEM_PROMPT = """You are an on-call incident response agent. When you receive alerts about service errors:

1. FIRST, make PARALLEL calls to fetch_logs() and compute_error_rate() simultaneously to gather data efficiently
2. Analyze the logs to identify the error type and root cause
3. If it's a code-related error (like NPE), call fetch_code_from_github() with the relevant file path
4. Provide a detailed diagnosis using send_diagnosis() with:
   - Root cause analysis
   - Exact line causing the issue
   - Suggested fix
   - Priority level
   - Next steps

IMPORTANT: Always call fetch_logs() and compute_error_rate() in parallel in your first response to maximize efficiency. You can make multiple function calls in a single response.

Be thorough but concise. Focus on actionable insights.

HUMAN-IN-THE-LOOP: If a user sends a message asking to create a Jira ticket/task (e.g., "create Jira task", "assign to Ravi", "create ticket"), immediately call create_jira_ticket() with the appropriate parameters extracted from the user's message and incident context."""

# Function definitions for OpenAI
FUNCTION_DEFINITIONS = [
    {
        "name": "fetch_logs",
        "description": "Fetch recent error logs from the service",
        "parameters": {
            "type": "object",
            "properties": {
                "seconds": {
                    "type": "integer",
                    "description": "Number of seconds to look back for logs",
                    "default": 300
                },
                "limit": {
                    "type": "integer", 
                    "description": "Maximum number of log entries to fetch",
                    "default": 20
                }
            }
        }
    },
    {
        "name": "compute_error_rate",
        "description": "Compute the current error rate from Prometheus metrics",
        "parameters": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "fetch_code_from_github",
        "description": "Fetch specific files from GitHub repository for code analysis",
        "parameters": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the file in the repository (e.g., 'inventory_service.py')"
                }
            },
            "required": ["file_path"]
        }
    },
    {
        "name": "send_diagnosis",
        "description": "Send the final diagnosis and recommendations",
        "parameters": {
            "type": "object",
            "properties": {
                "root_cause": {
                    "type": "string",
                    "description": "Root cause analysis of the error"
                },
                "affected_file": {
                    "type": "string",
                    "description": "File where the error occurred"
                },
                "line_number": {
                    "type": "integer",
                    "description": "Line number causing the issue (if applicable)"
                },
                "suggested_fix": {
                    "type": "string",
                    "description": "Detailed fix recommendation"
                },
                "priority": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "critical"],
                    "description": "Priority level of the issue"
                },
                "next_steps": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of recommended next steps"
                }
            },
            "required": ["root_cause", "affected_file", "suggested_fix", "priority", "next_steps"]
        }
    },
    {
        "name": "create_jira_ticket",
        "description": "Create a Jira ticket for the incident",
        "parameters": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string",
                    "description": "Short title for the Jira ticket"
                },
                "description": {
                    "type": "string",
                    "description": "Detailed description of the issue"
                },
                "assignee": {
                    "type": "string",
                    "description": "Username to assign the ticket to (e.g., 'ravi', 'john.doe')"
                },
                "priority": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "critical"],
                    "description": "Priority level for the ticket"
                }
            },
            "required": ["summary", "description"]
        }
    }
]

async def fetch_recent_logs(seconds: int = 300, limit: int = 2000) -> List[Dict[str, Any]]:
    """Fetch recent logs from the load balancer"""
    url = f"{LB_URL}/logs/tail?seconds={seconds}&limit={limit}"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            return data.get("entries", [])
    except Exception as e:
        print(f"Error fetching logs: {e}")
        return []

async def prom_instant_query(expr: str) -> float:
    """Query Prometheus for instant metrics"""
    q = {"query": expr}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{PROM_URL}/api/v1/query", params=q)
            r.raise_for_status()
            js = r.json()
            res = js.get("data", {}).get("result", [])
            if not res:
                return 0.0
            val = float(res[0].get("value", [0, "0"])[1])
            return val
    except Exception as e:
        print(f"Error querying Prometheus: {e}")
        return 0.0

async def compute_error_rate() -> float:
    """Compute the current error rate from Prometheus metrics"""
    num = await prom_instant_query('sum(rate(http_errors_total{job="inventory"}[1m]))')
    den = await prom_instant_query('sum(rate(http_requests_total{job="inventory"}[1m]))')
    if den <= 0.0:
        return 0.0
    return num / den

async def fetch_code_from_github(file_path: str) -> Dict[str, Any]:
    """Fetch specific file from GitHub repository"""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{file_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, headers=headers, params={"ref": GITHUB_REF})
            r.raise_for_status()
            data = r.json()
            
            # Decode base64 content
            content = base64.b64decode(data["content"]).decode("utf-8")
            
            return {
                "file_path": file_path,
                "content": content,
                "size": data["size"],
                "sha": data["sha"],
                "url": data["html_url"]
            }
    except Exception as e:
        print(f"Error fetching code from GitHub: {e}")
        return {
            "file_path": file_path,
            "content": f"Error fetching file: {str(e)}",
            "size": 0,
            "sha": "",
            "url": ""
        }

async def call_openai_with_functions(messages: List[Dict[str, str]]) -> Dict[str, Any]:
    """Call OpenAI with function calling enabled"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=[{"type": "function", "function": func} for func in FUNCTION_DEFINITIONS],
            tool_choice="auto",
            temperature=0.1
        )
        return response
    except Exception as e:
        print(f"Error calling OpenAI: {e}")
        return None

async def execute_function_call(function_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Execute the function call requested by the LLM"""
    if function_name == "fetch_logs":
        seconds = arguments.get("seconds", 300)
        limit = arguments.get("limit", 2000)
        logs = await fetch_recent_logs(seconds, limit)
        return {"logs": logs, "count": len(logs)}
    
    elif function_name == "compute_error_rate":
        error_rate = await compute_error_rate()
        return {"error_rate": error_rate}
    
    elif function_name == "fetch_code_from_github":
        file_path = arguments.get("file_path")
        if not file_path:
            return {"error": "file_path is required"}
        code_data = await fetch_code_from_github(file_path)
        return code_data
    
    elif function_name == "send_diagnosis":
        return {"status": "diagnosis_received", "data": arguments}
    
    elif function_name == "create_jira_ticket":
        summary = arguments.get("summary", "Incident Task")
        description = arguments.get("description", "No description provided")
        assignee = arguments.get("assignee")
        priority = arguments.get("priority", "medium")
        
        jira_result = await create_jira_issue(summary, description, assignee)
        return jira_result
    
    else:
        return {"error": f"Unknown function: {function_name}"}

async def execute_parallel_function_calls(function_calls: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Execute multiple function calls in parallel"""
    import asyncio
    
    async def execute_single_call(call):
        function_name = call["name"]
        arguments = call.get("arguments", {})
        result = await execute_function_call(function_name, arguments)
        return {
            "function_name": function_name,
            "arguments": arguments,
            "result": result
        }
    
    # Execute all function calls in parallel
    tasks = [execute_single_call(call) for call in function_calls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle any exceptions
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            processed_results.append({
                "function_name": function_calls[i]["name"],
                "arguments": function_calls[i].get("arguments", {}),
                "result": {"error": str(result)}
            })
        else:
            processed_results.append(result)
    
    return processed_results

async def send_whatsapp_message(message: str, to_number: str = None) -> bool:
    """Send WhatsApp message via Twilio"""
    if not twilio_client:
        print("Twilio client not initialized - skipping WhatsApp message")
        return False
    
    try:
        to_number = to_number or USER_WHATSAPP_NUMBER
        message_obj = twilio_client.messages.create(
            body=message,
            from_=TWILIO_WHATSAPP_NUMBER,
            to=to_number
        )
        print(f"WhatsApp message sent: {message_obj.sid}")
        return True
    except Exception as e:
        print(f"Error sending WhatsApp message: {e}")
        return False

async def create_jira_issue(summary: str, description: str, assignee: str = None) -> Dict[str, Any]:
    """Create a Jira issue"""
    if not all([JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN]):
        return {"error": "Jira credentials not configured"}
    
    try:
        import base64
        auth_string = f"{JIRA_EMAIL}:{JIRA_API_TOKEN}"
        auth_bytes = auth_string.encode('ascii')
        auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
        
        headers = {
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/json"
        }
        
        issue_data = {
            "fields": {
                "project": {"key": JIRA_PROJECT_KEY},
                "summary": summary,
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [
                                {
                                    "type": "text",
                                    "text": description
                                }
                            ]
                        }
                    ]
                },
                "issuetype": {"name": "Task"}
            }
        }
        
        if assignee:
            issue_data["fields"]["assignee"] = {"name": assignee}
        
        # Ensure JIRA_URL doesn't end with slash to avoid double slash
        jira_url = JIRA_URL.rstrip('/')
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{jira_url}/rest/api/3/issue",
                headers=headers,
                json=issue_data
            )
            response.raise_for_status()
            result = response.json()
            
            return {
                "success": True,
                "issue_key": result["key"],
                "issue_url": f"{JIRA_URL}/browse/{result['key']}",
                "issue_id": result["id"]
            }
    except Exception as e:
        print(f"Error creating Jira issue: {e}")
        return {"error": str(e)}

def format_diagnosis_for_whatsapp(diagnosis: Dict[str, Any]) -> str:
    """Format diagnosis for WhatsApp message"""
    return f"""🚨 *Incident Diagnosis*

*Root Cause:* {diagnosis.get('root_cause', 'Unknown')}
*File:* {diagnosis.get('affected_file', 'Unknown')}
*Priority:* {diagnosis.get('priority', 'Unknown').upper()}

*Suggested Fix:*
{diagnosis.get('suggested_fix', 'No fix suggested')}

*Next Steps:*
{chr(10).join(f"• {step}" for step in diagnosis.get('next_steps', []))}

Reply with your action (e.g., "create Jira task and assign to Ravi")"""

def store_conversation(conversation_id: str, messages: List[Dict], diagnosis: Dict[str, Any] = None):
    """Store conversation in memory"""
    conversation_storage[conversation_id] = {
        "messages": messages,
        "diagnosis": diagnosis,
        "timestamp": time.time()
    }

def get_conversation(conversation_id: str) -> Dict[str, Any]:
    """Retrieve conversation from memory"""
    return conversation_storage.get(conversation_id, {})

@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "on_call_agent"}

@app.post("/alerts")
async def alerts(req: Request):
    """Main alert processing endpoint with OpenAI function calling"""
    payload = await req.json()
    print("==== Alertmanager webhook ====")
    print(payload)
    
    # Prepare initial message for LLM
    alert_data = payload.get("alerts", [{}])[0] if payload.get("alerts") else {}
    alert_labels = alert_data.get("labels", {})
    alert_annotations = alert_data.get("annotations", {})
    
    initial_message = f"""Alert received:
- Alert Name: {alert_labels.get('alertname', 'Unknown')}
- Severity: {alert_labels.get('severity', 'Unknown')}
- Service: {alert_labels.get('service', 'Unknown')}
- Summary: {alert_annotations.get('summary', 'No summary')}
- Description: {alert_annotations.get('description', 'No description')}

Please analyze this alert and provide a diagnosis."""
    
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": initial_message}
    ]
    
    # Start conversation with LLM
    max_iterations = 10
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        print(f"\n=== LLM Iteration {iteration} ===")
        
        # Call OpenAI
        response = await call_openai_with_functions(messages)
        if not response:
            break
            
        message = response.choices[0].message
        print(f"LLM Response: {message.content}")
        
        # Extract function calls from LLM response
        function_calls = []
        
        # Check for tool calls (new OpenAI API format)
        if message.tool_calls:
            for tool_call in message.tool_calls:
                try:
                    arguments = json.loads(tool_call.function.arguments) if tool_call.function.arguments else {}
                except json.JSONDecodeError:
                    arguments = {}
                function_calls.append({
                    "name": tool_call.function.name,
                    "arguments": arguments
                })
        
        if function_calls:
            print(f"Function Calls: {[fc['name'] for fc in function_calls]}")
            
            # Execute all function calls in parallel
            parallel_results = await execute_parallel_function_calls(function_calls)
            print(f"Function Results: {parallel_results}")
            
            # Add function calls and results to conversation
            messages.append({
                "role": "assistant",
                "content": message.content,
                "tool_calls": [
                    {
                        "id": f"call_{i}",
                        "type": "function",
                        "function": {
                            "name": fc["name"],
                            "arguments": json.dumps(fc["arguments"])
                        }
                    } for i, fc in enumerate(function_calls)
                ]
            })
            
            # Add each function result
            for i, result in enumerate(parallel_results):
                messages.append({
                    "role": "tool",
                    "tool_call_id": f"call_{i}",
                    "content": json.dumps(result["result"])
                })
            
            # Check if any function was send_diagnosis
            if any(fc["name"] == "send_diagnosis" for fc in function_calls):
                diagnosis_result = next(r for r in parallel_results if r["function_name"] == "send_diagnosis")
                print("==== Final Diagnosis ====")
                print(json.dumps(diagnosis_result["result"], indent=2))
                
                # Store conversation and send WhatsApp message
                conversation_id = str(uuid.uuid4())
                store_conversation(conversation_id, messages, diagnosis_result["result"]["data"])
                
                # Send WhatsApp message
                whatsapp_message = format_diagnosis_for_whatsapp(diagnosis_result["result"]["data"])
                await send_whatsapp_message(whatsapp_message)
                
                print(f"Conversation stored with ID: {conversation_id}")
                break
                
        else:
            # No function call, just add the response
            messages.append({"role": "assistant", "content": message.content})
            break
    
    return {"ok": True, "iterations": iteration, "final_response": messages[-1] if messages else None}

@app.post("/webhook/twilio")
async def twilio_webhook(req: Request):
    """Handle incoming WhatsApp messages from Twilio"""
    form_data = await req.form()
    
    # Extract message details from Twilio webhook
    from_number = form_data.get("From")
    message_body = form_data.get("Body", "").strip()
    
    print(f"==== WhatsApp Message Received ====")
    print(f"From: {from_number}")
    print(f"Message: {message_body}")
    
    # Get the latest conversation (since we're only handling one conversation)
    if not conversation_storage:
        await send_whatsapp_message("No active incident conversation found.")
        return {"ok": True}
    
    # Get the most recent conversation
    latest_conversation_id = max(conversation_storage.keys(), key=lambda k: conversation_storage[k]["timestamp"])
    conversation = get_conversation(latest_conversation_id)
    
    if not conversation:
        await send_whatsapp_message("Conversation not found.")
        return {"ok": True}
    
    # Prepare context for LLM with user message
    diagnosis = conversation.get("diagnosis", {})
    context_message = f"""User message: "{message_body}"

Incident context:
- Root Cause: {diagnosis.get('root_cause', 'Unknown')}
- Affected File: {diagnosis.get('affected_file', 'Unknown')}
- Priority: {diagnosis.get('priority', 'Unknown')}

Please analyze the user's message and take appropriate action. If they're asking to create a Jira ticket, use the create_jira_ticket function."""
    
    # Add user message to conversation and get LLM response
    messages = conversation.get("messages", [])
    messages.append({"role": "user", "content": context_message})
    
    # Get LLM response with function calling
    max_iterations = 5
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        print(f"\n=== LLM Iteration {iteration} (User Response) ===")
        
        # Call OpenAI
        response = await call_openai_with_functions(messages)
        if not response:
            break
            
        message = response.choices[0].message
        print(f"LLM Response: {message.content}")
        
        # Extract function calls from LLM response
        function_calls = []
        
        # Check for tool calls (new OpenAI API format)
        if message.tool_calls:
            for tool_call in message.tool_calls:
                try:
                    arguments = json.loads(tool_call.function.arguments) if tool_call.function.arguments else {}
                except json.JSONDecodeError:
                    arguments = {}
                function_calls.append({
                    "name": tool_call.function.name,
                    "arguments": arguments
                })
        
        if function_calls:
            print(f"Function Calls: {[fc['name'] for fc in function_calls]}")
            
            # Execute all function calls in parallel
            parallel_results = await execute_parallel_function_calls(function_calls)
            print(f"Function Results: {parallel_results}")
            
            # Add function calls and results to conversation
            messages.append({
                "role": "assistant",
                "content": message.content,
                "tool_calls": [
                    {
                        "id": f"call_{i}",
                        "type": "function",
                        "function": {
                            "name": fc["name"],
                            "arguments": json.dumps(fc["arguments"])
                        }
                    } for i, fc in enumerate(function_calls)
                ]
            })
            
            # Add each function result
            for i, result in enumerate(parallel_results):
                messages.append({
                    "role": "tool",
                    "tool_call_id": f"call_{i}",
                    "content": json.dumps(result["result"])
                })
            
            # Check if any function was create_jira_ticket
            if any(fc["name"] == "create_jira_ticket" for fc in function_calls):
                jira_result = next(r for r in parallel_results if r["function_name"] == "create_jira_ticket")
                if jira_result["result"].get("success"):
                    response_message = f"""✅ *Jira Task Created Successfully*

*Task:* {jira_result['result']['issue_key']}
*URL:* {jira_result['result']['issue_url']}
*Assignee:* {function_calls[0]['arguments'].get('assignee', 'Unassigned')}

The incident has been logged and assigned for resolution."""
                else:
                    response_message = f"❌ Failed to create Jira task: {jira_result['result'].get('error', 'Unknown error')}"
                
                await send_whatsapp_message(response_message)
                break
                
        else:
            # No function call, just send the response
            await send_whatsapp_message(message.content)
            break
    
    return {"ok": True}


@app.post("/human/decision")
async def human_decision(req: Request):
    """Placeholder for human interaction"""
    body = await req.json()
    print("==== Human decision ====")
    print(body)
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=AGENT_PORT)