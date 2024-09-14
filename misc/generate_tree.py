import matplotlib.pyplot as plt

# Step 2: Define the nodes and edges
nodes = [
    "HEADER", "FUNCTIONAL GROUPS", "STATEMENTS", 
    "STATEMENTS_REFS","STATEMENTS_CAS","STATEMENTS_LQ","STATEMENTS_DTM",
    "CLAIMS","CLAIMS_CAS", "CLAIMS_REF", "CLAIMS_DTM",
    "SERVICES", "SERVICES_CAS", "SERVICES_REF", "SERVICES_DTM"
]

edges = [
    ("HEADER", "FUNCTIONAL GROUPS"),
    ("FUNCTIONAL GROUPS", "STATEMENTS"),
    ("STATEMENTS_REFS", "STATEMENTS"),
    ("STATEMENTS_CAS", "STATEMENTS"),
    ("STATEMENTS_LQ", "STATEMENTS"),
    ("STATEMENTS_DTM", "STATEMENTS"),
    ("STATEMENTS", "CLAIMS"),
    ("CLAIMS_CAS", "CLAIMS"),
    ("CLAIMS_REF", "CLAIMS"),
    ("CLAIMS_DTM", "CLAIMS"),
    ("CLAIMS_LQ", "CLAIMS"),
    ("CLAIMS", "SERVICES"),
    ("SERVICES_CAS", "SERVICES"),
    ("SERVICES_REF", "SERVICES"),
    ("SERVICES_DTM", "SERVICES"),
    ("SERVICES_LQ", "SERVICES")
]

# Step 3: Define positions for the nodes
positions = {
    "HEADER": (0, 4),
    "FUNCTIONAL GROUPS": (0, 3),
    "STATEMENTS": (0, 2),
    "STATEMENTS_CAS": (-1, 1.5),
    "STATEMENTS_REFS": (1, 1.5),
    "STATEMENTS_DTM": (-0.5, 1.5),
    "STATEMENTS_LQ": (0.5, 1.5),
    "CLAIMS": (0, 1),
    "CLAIMS_CAS": (-1, 0),
    "CLAIMS_REF": (1, 0),
    "CLAIMS_DTM": (-0.5, 0),
    "CLAIMS_LQ": (0.5, 0),
    "SERVICES": (0, -1),
    "SERVICES_CAS": (-1, -2),
    "SERVICES_REF": (1, -2),
    "SERVICES_DTM": (-0.5, -2),
    "SERVICES_LQ": (0.5, -2)
}

# Step 4: Plot the graph
fig, ax = plt.subplots(figsize=(10, 10))

# Plot nodes
for node, (x, y) in positions.items():
    ax.scatter(x, y, s=0)
    ax.text(x, y, node, fontsize=12, ha='center', va='center')

# Plot edges with padding
for start, end in edges:
    start_pos = positions[start]
    end_pos = positions[end]
    # Calculate the direction vector
    direction = (end_pos[0] - start_pos[0], end_pos[1] - start_pos[1])
    # Normalize the direction vector
    length = (direction[0]**2 + direction[1]**2)**0.5
    direction = (direction[0] / length, direction[1] / length)
    # Add padding
    padding = 0.2
    start_pos_padded = (start_pos[0] + direction[0] * padding, start_pos[1] + direction[1] * padding)
    end_pos_padded = (end_pos[0] - direction[0] * padding, end_pos[1] - direction[1] * padding)
    ax.annotate("",
                xy=end_pos_padded, xycoords='data',
                xytext=start_pos_padded, textcoords='data',
                arrowprops=dict(arrowstyle='-|>', lw=2))

plt.axis('off')  # Turn off the axis
# Save the plot to a PNG file
plt.savefig("tree_structure.png", format="png", bbox_inches="tight")

plt.show()