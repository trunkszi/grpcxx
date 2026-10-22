@0xe59cbe736248ad15;
using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("examples::order_book::capnp");

struct QuoteRequest { id @0 :Int64; symbol @1 :Text; }
struct QuoteResponse { id @0 :Int64; price @1 :Int64; server @2 :Int64; }
struct OrderRequest { id @0 :Int64; symbol @1 :Text; quantity @2 :Int64; }
struct OrderResponse { id @0 :Int64; quantity @1 :Int64; server @2 :Int64; }

interface OrderBook {
  quote @0 (request :QuoteRequest) -> (response :QuoteResponse);
  placeOrder @1 (request :OrderRequest) -> (response :OrderResponse);
}
