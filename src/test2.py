import airbyte as ab
from pathlib import Path  

source = ab.get_source(  
    "source-bigcommerce",  
    local_executable=Path("/home/edwin/Source/python/airbyte_env/lib/python3.12/site-packages"),  # Specific path  
    config={  
        "store_hash": "vruswqw1od",  
        "access_token": "kejj9lcr6bs90emjhzd8bbcylcp3vi1",  
    }  
)  

# Validate the connection  
source.check() 