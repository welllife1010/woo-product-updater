// __tests__/create-missing-products.test.js
const { __test } = require("../create-missing-products");
const {
  parseCategoryPath,
  buildCategoryPath,
} = __test;

describe("create-missing-products category utilities", () => {
  test("parseCategoryPath splits 'A>B>C' into main/sub/sub2", () => {
    const raw = "Integrated Circuits (ICs)>Embedded>Microcontrollers";
    const result = parseCategoryPath(raw);

    expect(result).toEqual({
      main: "Integrated Circuits (ICs)",
      sub: "Embedded",
      sub2: "Microcontrollers",
      score: 1,
      matchedOn: "sub2",
    });
  });

  test("buildCategoryPath formats a resolvedCategory to 'A > B > C'", () => {
    const input = {
      main: "Integrated Circuits (ICs)",
      sub: "Embedded",
      sub2: "Microcontrollers",
    };

    const path = buildCategoryPath(input);
    expect(path).toBe("Integrated Circuits (ICs) > Embedded > Microcontrollers");
  });
});
