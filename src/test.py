# Import PyAirbyte
import airbyte as ab

# Show all available connectors
# results = ab.get_available_connectors()

#if "source-bigcommerce" in results:
#    print("Found bigcommerce source Connector!")
#else:
#    print("You're out of luck!")

print(ab.get_connector("source-bigcommerce"))