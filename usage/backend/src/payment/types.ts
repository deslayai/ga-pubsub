export type PaymentStatus =
  | 'created' | 'authorizing' | 'authorized'
  | 'processing' | 'completed' | 'failed' | 'refunded' | 'expired';

export type PaymentMethod = 'card' | 'bank_transfer' | 'wallet' | 'crypto';

export interface Payment {
  id:          string;
  amount:      number;
  currency:    string;
  method:      PaymentMethod;
  status:      PaymentStatus;
  fromUserId:  string;
  toMerchant:  string;
  description: string;
  createdAt:   number;
  updatedAt:   number;
  metadata?:   Record<string, unknown>;
}

export interface PaymentEvent {
  payment: Payment;
  previousStatus?: PaymentStatus;
  message?:        string;
}

export interface InvoicePayload {
  invoiceId:    string;
  paymentId:    string;
  amount:       number;
  currency:     string;
  lineItems:    Array<{ description: string; amount: number }>;
  dueDate:      number;
}

export interface FraudSignal {
  paymentId: string;
  riskScore: number;        // 0–1
  reason:    string;
  blocked:   boolean;
}

// Dummy data helpers
const MERCHANTS = ['ShopEasy', 'CloudStore', 'TechMart', 'FoodExpress', 'StreamPro'];
const METHODS: PaymentMethod[] = ['card', 'bank_transfer', 'wallet', 'crypto'];
const AMOUNTS = [9.99, 19.99, 49.99, 99.00, 149.50, 199.99, 299.00, 499.99, 999.00];

export function randomPayment(overrides: Partial<Payment> = {}): Payment {
  const id = `pay_${Math.random().toString(36).slice(2, 10)}`;
  return {
    id,
    amount:      AMOUNTS[Math.floor(Math.random() * AMOUNTS.length)]!,
    currency:    'USD',
    method:      METHODS[Math.floor(Math.random() * METHODS.length)]!,
    status:      'created',
    fromUserId:  `user_${Math.random().toString(36).slice(2, 8)}`,
    toMerchant:  MERCHANTS[Math.floor(Math.random() * MERCHANTS.length)]!,
    description: `Payment for order #${Math.floor(Math.random() * 90000) + 10000}`,
    createdAt:   Date.now(),
    updatedAt:   Date.now(),
    ...overrides,
  };
}
