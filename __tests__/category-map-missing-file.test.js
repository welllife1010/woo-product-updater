describe("category-map (missing CSV)", () => {
  const ORIGINAL_ENV = process.env;

  beforeEach(() => {
    jest.resetModules();
    process.env = { ...ORIGINAL_ENV };
  });

  afterAll(() => {
    process.env = ORIGINAL_ENV;
  });

  test("loadCategoryHierarchy resolves empty array when CSV path is missing", async () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});
    process.env.CATEGORY_HIERARCHY_CSV_PATH = "/tmp/definitely-does-not-exist/category-hierarchy-ref.csv";

    const { loadCategoryHierarchy, resolveCategory } = require("../src/resolvers/category-map");

    await expect(loadCategoryHierarchy()).resolves.toEqual([]);
    await expect(resolveCategory("Some Category")).resolves.toBeNull();

    warnSpy.mockRestore();
  });
});
