// __tests__/category-woo.test.js

// 1) Mock woo-helpers BEFORE requiring category-woo
jest.mock("../woo-helpers", () => ({
  wooApi: {
    get: jest.fn(),
    post: jest.fn(),
  },
}));

const { wooApi } = require("../woo-helpers");
const { ensureCategoryHierarchy } = require("../category-woo");

describe("category-woo.ensureCategoryHierarchy", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("creates main → sub → sub2 when none exist", async () => {
    // No existing categories in Woo
    wooApi.get.mockResolvedValue({ data: [] });

    // When creating categories, generate simple fake IDs
    let nextId = 100;
    wooApi.post.mockImplementation((endpoint, body) => {
      return Promise.resolve({
        data: {
          id: nextId++,
          name: body.name,
          parent: body.parent,
        },
      });
    });

    const resolved = {
      main: "Integrated Circuits (ICs)",
      sub: "Embedded",
      sub2: "Microcontrollers",
    };

    const ids = await ensureCategoryHierarchy(resolved);

    // We should get 3 IDs back
    expect(ids).toHaveLength(3);

    // Should have called Woo POST three times (main, sub, sub2)
    expect(wooApi.post).toHaveBeenCalledTimes(3);

    // Check that the parent chain is correct
    const [firstCall, secondCall, thirdCall] = wooApi.post.mock.calls;

    // 1) main: parent=0
    expect(firstCall[0]).toBe("products/categories");
    expect(firstCall[1]).toMatchObject({ name: "Integrated Circuits (ICs)", parent: 0 });

    // 2) sub: parent = ID returned for main (100)
    expect(secondCall[1]).toMatchObject({ name: "Embedded", parent: 100 });

    // 3) sub2: parent = ID for sub (101)
    expect(thirdCall[1]).toMatchObject({ name: "Microcontrollers", parent: 101 });
  });

  test("reuses existing categories and does not POST again", async () => {
    // Simulate Woo already having the full chain
    wooApi.get.mockResolvedValue({
      data: [
        { id: 1, name: "Integrated Circuits (ICs)", parent: 0 },
        { id: 2, name: "Embedded", parent: 1 },
        { id: 3, name: "Microcontrollers", parent: 2 },
      ],
    });

    const resolved = {
      main: "Integrated Circuits (ICs)",
      sub: "Embedded",
      sub2: "Microcontrollers",
    };

    const ids = await ensureCategoryHierarchy(resolved);

    // We should get the existing IDs
    expect(ids).toEqual([1, 2, 3]);

    // No new categories created
    expect(wooApi.post).not.toHaveBeenCalled();
  });
});
