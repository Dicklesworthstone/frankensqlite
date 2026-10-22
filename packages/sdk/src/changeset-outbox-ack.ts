import type { OutboxDelivery } from "./changeset-outbox-store";
import { digest, fail, identity } from "./changeset-outbox-store";

/** Exact identities whose remote confirmation and omission policy are already verified. */
export type OutboxAcknowledgement = Readonly<Pick<OutboxDelivery, "deliveryId" | "sha256">>;

/** @internal Capture a bounded ACK window before SQL admission or any await. */
export function captureAcknowledgements(values: readonly OutboxAcknowledgement[]): readonly OutboxAcknowledgement[] {
  if (!Array.isArray(values)) fail("INPUT", "An array of 1..256 acknowledgements is required");
  const length: unknown = Object.getOwnPropertyDescriptor(values, "length")?.value;
  if (typeof length !== "number" || !Number.isSafeInteger(length) || length < 1 || length > 256)
    fail("INPUT", "Use 1..256 acknowledgements");
  const own = (value: unknown, name: string): unknown => {
    if (typeof value !== "object" || value === null || Array.isArray(value))
      fail("INPUT", "An acknowledgement record is required");
    const descriptor = Object.getOwnPropertyDescriptor(value, name);
    if (descriptor === undefined || !Object.hasOwn(descriptor, "value"))
      fail("INPUT", "Acknowledgement fields must be own data properties");
    return descriptor.value;
  };
  const result: OutboxAcknowledgement[] = [], ids = new Set<string>();
  for (let i = 0; i < length; i++) {
    const item = Object.getOwnPropertyDescriptor(values, String(i));
    if (item === undefined || !Object.hasOwn(item, "value"))
      fail("INPUT", "Acknowledgements must not contain holes or element accessors");
    const deliveryId = identity(own(item.value, "deliveryId"));
    const sha256 = digest(own(item.value, "sha256"));
    if (ids.has(deliveryId)) fail("INPUT", "Acknowledgements must have distinct delivery identities");
    ids.add(deliveryId);
    result.push(Object.freeze({ deliveryId, sha256 }));
  }
  return Object.freeze(result);
}
