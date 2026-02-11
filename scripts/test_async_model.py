
import asyncio
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2
from src.database import get_async_db
from sqlalchemy import text

async def main():
    print("Initializing Async Model...")
    model = NCAAMMarketFirstModelV2()
    
    print("Testing DB Connection...")
    async for session in get_async_db():
        try:
            # Simple query to verify asyncpg
            res = await session.execute(text("SELECT 1"))
            val = res.scalar()
            print(f"DB Success! Value: {val}")
            
            # Mock Event ID for analysis (might fail if data missing, but tests path)
            # await model.analyze("action:ncaam:123", session, {})
            # print("Analysis path check passed (mocked)")
            
        except Exception as e:
            print(f"DB Failure: {e}")
            import traceback
            traceback.print_exc()
        break
        
if __name__ == "__main__":
    asyncio.run(main())
