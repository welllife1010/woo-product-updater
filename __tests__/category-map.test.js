// __tests__/category-map.test.js

const {
  resolveCategory,
  __setCategoryRowsForTest,
} = require("../category-map");

describe("category-map.js - resolveCategory with Fuse.js", () => {
  beforeAll(() => {
    // Simulate a tiny subset of your category hierarchy CSV.
    __setCategoryRowsForTest([
      {
        mainRaw: "Capacitors",
        subRaw: "Film Capacitors",
        sub2Raw: "",
      },
      {
        mainRaw: "Connectors, Interconnects",
        subRaw: "Circular Cable Assemblies",
        sub2Raw: "",
      },
      {
        mainRaw: "Cables, Wires",
        subRaw: "Fiber Optic Cables",
        sub2Raw: "",
      },
    ]);
  });

  test("exact sub-category match returns expected main/sub", async () => {
    const match = await resolveCategory("Film Capacitors");

    expect(match).not.toBeNull();
    expect(match.main).toBe("Capacitors");
    expect(match.sub).toBe("Film Capacitors");
    expect(match.sub2).toBeNull(); // no 2nd sub category in our test data
    expect(["sub", "main"]).toContain(match.matchedOn); // usually "sub"
    expect(match.score).toBeGreaterThan(0.7); // strong similarity
  });

  test("noisy vendor label still matches the right sub category", async () => {
    const match = await resolveCategory(
      "Circular Cable Assemblies (Shenzhen Signal)"
    );

    expect(match).not.toBeNull();
    expect(match.main).toBe("Connectors, Interconnects");
    expect(match.sub).toBe("Circular Cable Assemblies");
    expect(match.sub2).toBeNull();
    expect(match.matchedOn).toBe("sub");
    // Our MIN_SIMILARITY in category-map.js is 0.4,
    // and in practice this noisy label scores around ~0.42.
    expect(match.score).toBeGreaterThan(0.4);
  });

  test("typo / partial text still finds a good match", async () => {
    const match = await resolveCategory("fiber optc cabls");

    expect(match).not.toBeNull();
    expect(match.main).toBe("Cables, Wires");
    expect(match.sub).toBe("Fiber Optic Cables");
    expect(match.matchedOn).toBe("sub");
    expect(match.score).toBeGreaterThan(0.4); // may be lower due to typos
  });

  test("completely unrelated text returns null", async () => {
    const match = await resolveCategory("Potato Chips");

    expect(match).toBeNull();
  });
});
