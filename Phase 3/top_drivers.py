driver_stats = {
    "D102": {"rides": 3, "earnings": 15},
    "D101": {"rides": 2, "earnings": 10},
    "D104": {"rides": 1, "earnings": 5}
}

sorted_drivers = sorted(
    driver_stats.items(),
    key=lambda x: x[1]["rides"],
    reverse=True
)

print("\nTop Drivers\n")

for driver_id, stats in sorted_drivers[:5]:
    print(
        f"{driver_id} | "
        f"Rides: {stats['rides']} | "
        f"Earnings: ${stats['earnings']}"
    )