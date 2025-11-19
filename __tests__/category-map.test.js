// __tests__/category-map.test.js
const {
  resolveCategory,
  __setCategoryRowsForTest,
} = require("../category-map");

describe("category-map.resolveCategory", () => {
  beforeEach(() => {
    // Give it a tiny fake hierarchy instead of the real CSV
    __setCategoryRowsForTest([
      {
        mainRaw: "Integrated Circuits (ICs)",
        subRaw: "Embedded",
        sub2Raw: "Microcontrollers",
        main: "integrated circuits (ics)",
        sub: "embedded",
        sub2: "microcontrollers",
      },
      {
        mainRaw: "Passive Components",
        subRaw: "Capacitors",
        sub2Raw: "Ceramic",
        main: "passive components",
        sub: "capacitors",
        sub2: "ceramic",
      },
    ]);
  });

  test("exact match returns the correct hierarchy", async () => {
    const raw = "Integrated Circuits (ICs)>Embedded>Microcontrollers";

    const result = await resolveCategory(raw);

    expect(result).toMatchObject({
      main: "Integrated Circuits (ICs)",
      sub: "Embedded",
      sub2: "Microcontrollers",
    });
  });

  test("fuzzy match still finds the correct row", async () => {
    // Deliberately a bit messy / different from the canonical label
    const raw = "Integrated circuits ics  microcontroller";

    const result = await resolveCategory(raw);

    // As long as Fuse thinks this is closest to our MCU row,
    // we should get that hierarchy back.
    expect(result.main).toBe("Integrated Circuits (ICs)");
    expect(result.sub).toBe("Embedded");
    expect(result.sub2).toBe("Microcontrollers");
    expect(result.score).toBeLessThan(0.4); // or whatever threshold you're using
  });

  test("returns null when nothing reasonable matches", async () => {
    const raw = "Totally Unrelated Category ABC 123";

    const result = await resolveCategory(raw);

    expect(result).toBeNull();
  });
});
