import duckdb
import matplotlib.pyplot as plt

con = duckdb.connect("identifier.db")

df = con.execute("""
SELECT EXTRACT(YEAR FROM release_date) AS year, COUNT(*) AS games
FROM steam_games_clean
WHERE release_date IS NOT NULL
GROUP BY year
ORDER BY year;
""").fetchdf()

plt.figure(figsize=(10,6))
plt.bar(df['year'], df['games'], color='steelblue')
plt.title("Steam Games Released per Year")
plt.xlabel("Year")
plt.ylabel("Number of Games")
plt.tight_layout()
plt.show()
