"""
Test script to verify all generators are properly registered and can be instantiated
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.generator_factory import GeneratorFactory, GeneratorMethod
from loguru import logger


def test_generator_registration():
    """Test that all generators are registered in the factory"""
    logger.info("Testing generator registration...")

    available_methods = GeneratorFactory.get_available_methods()
    logger.info(f"Available methods: {len(available_methods)} registered")

    # Extract method names from dictionaries
    available_method_names = [m["method"] for m in available_methods]

    expected_methods = [
        GeneratorMethod.CTGAN,
        GeneratorMethod.TVAE,
        GeneratorMethod.GAUSSIAN_COPULA,
        GeneratorMethod.SMOTE,
    ]

    for method in expected_methods:
        if method in available_method_names:
            logger.success(f"✓ {method} is registered")
        else:
            logger.error(f"✗ {method} is NOT registered")

    return set(available_method_names) == set(expected_methods)


def test_generator_creation():
    """Test that each generator can be instantiated"""
    logger.info("\nTesting generator instantiation...")

    methods_info = GeneratorFactory.get_available_methods()
    methods = [m["method"] for m in methods_info]

    for method in methods:
        try:
            generator = GeneratorFactory.create(method, model_name="test_model")
            logger.success(f"✓ {method} generator created successfully")
            logger.info(f"  Type: {type(generator).__name__}")
        except Exception as e:
            logger.error(f"✗ Failed to create {method} generator: {e}")
            return False

    return True


def main():
    logger.info("=" * 60)
    logger.info("Testing All Synthetic Data Generators")
    logger.info("=" * 60)

    # Test registration
    registration_ok = test_generator_registration()

    # Test creation
    creation_ok = test_generator_creation()

    # Summary
    logger.info("\n" + "=" * 60)
    if registration_ok and creation_ok:
        logger.success("All tests passed! ✓")
        logger.info("\nAvailable generators:")
        methods_info = GeneratorFactory.get_available_methods()
        for method_info in methods_info:
            logger.info(f"  - {method_info['method']}: {method_info['name']}")
        return 0
    else:
        logger.error("Some tests failed! ✗")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
