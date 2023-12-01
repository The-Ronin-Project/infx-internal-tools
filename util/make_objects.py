from unittest import skip

import uuid


#@skip("This is a utility, not a test. Use only to generate a uuid string while locally testing objects.")
def test_make_uuid() -> str:
    result = str(uuid.uuid4())
    print(f"\n\n{result}")
    return result
