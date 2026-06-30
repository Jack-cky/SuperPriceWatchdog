from supabase import create_client
from supabase.client import ClientOptions

from .settings import Settings


def get_supabase_client():
    return create_client(
        Settings.SUPABASE_URL,
        Settings.SUPABASE_KEY,
        ClientOptions(
            schema=Settings.SUPABASE_SCHEMA,
            postgrest_client_timeout=30,
            function_client_timeout=30,
            storage_client_timeout=30,
        ),
    )


SUPABASE_CLIENT = get_supabase_client()
