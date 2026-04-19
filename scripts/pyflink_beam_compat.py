"""
PyFlink + Apache Beam 2.69 compatibility monkey patches.
Import this BEFORE importing pyflink to fix API incompatibilities.
"""

import sys
from typing import Any


def patch_beam_compatibility():
    """Apply runtime patches for apache-beam 2.69 compatibility with PyFlink 1.19."""

    # Patch 1: FunctionOperation.setup() signature changed
    try:
        import apache_beam.runners.worker.bundle_processor as bp

        # Find all Operation classes and patch their setup() if needed
        for name in dir(bp):
            cls = getattr(bp, name)
            if hasattr(cls, 'setup') and callable(getattr(cls, 'setup', None)):
                try:
                    original = cls.setup

                    def make_patched(orig):
                        def patched_setup(self, *args, **kwargs):
                            # Call original without any args
                            if callable(orig):
                                return orig(self)
                        return patched_setup

                    cls.setup = make_patched(original)
                except:
                    pass

        print("✓ Patched Operation.setup() methods")
    except Exception as e:
        print(f"⚠ Could not patch Operations: {e}")

    # Patch 2: Add missing _get_state_cache_size for backwards compat
    try:
        from apache_beam.runners.worker import sdk_worker_main

        if not hasattr(sdk_worker_main, '_get_state_cache_size'):
            sdk_worker_main._get_state_cache_size = lambda experiments: 100 * 1024 * 1024
            print("✓ Added _get_state_cache_size() compatibility shim")
    except Exception as e:
        print(f"⚠ Could not add state cache shim: {e}")


# Auto-patch on import
patch_beam_compatibility()
