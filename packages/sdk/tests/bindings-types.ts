// Compile-only public API contract. Never executed (includes intentional errors).
import type { FrankenDB, SqlBindings } from "../src/index";

function accepts<T>(_value: T): void {}

export async function bindingTypes(db: FrankenDB): Promise<void> {
  type User = { id: number; name: string };
  const values: SqlBindings = { id: 1 };
  accepts<Promise<number>>(db.execute("UPDATE users SET name = :name WHERE id = :id", { id: 1, name: "Ada" }));
  accepts<Promise<import("../src/types").QueryResult<User>>>(db.query<User>("SELECT * FROM users WHERE id = :id", values));
  const statement = await db.prepare<User>("SELECT id,name FROM users WHERE id = :id");
  accepts<Promise<typeof statement>>(statement.bind(values));
  accepts<Promise<User | undefined>>(statement.get());
  accepts<Promise<User[]>>(statement.all([1]));
  accepts<Promise<number>>(statement.run({ id: 1 }));
  accepts<Promise<void>>(statement.clearBindings());
  accepts<number>(statement.parameterCount);
  accepts<readonly (string | null)[]>(statement.parameterNames);
  await db.transaction(tx => tx.query<User>("SELECT * FROM users WHERE id = :id", { id: 1 }));
  // @ts-expect-error arbitrary nested objects are not SQL scalar bindings
  statement.bind({ id: { nested: true } });
  // @ts-expect-error result mapping preserves the declared row type
  accepts<Promise<{ other: boolean }[]>>(statement.all());
  // @ts-expect-error parameter metadata is read-only
  statement.parameterNames.push(":new");
  // @ts-expect-error bulk row sources stay positional, not named records
  statement.executeMany([{ id: 1 }]);
}
