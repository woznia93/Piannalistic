#include <pybind11/pybind11.h>

#include "pma/analytics.hpp"

namespace py = pybind11;
using namespace pma;

PYBIND11_MODULE(pma_core, m) {
    m.doc() = "C++ streaming market analytics core";

    py::class_<Snapshot>(m, "Snapshot")
        .def_readonly("ts", &Snapshot::ts)
        .def_readonly("last_price", &Snapshot::last_price)
        .def_readonly("mid", &Snapshot::mid)
        .def_readonly("spread_bps", &Snapshot::spread_bps)
        .def_readonly("quote_imbalance", &Snapshot::quote_imbalance)
        .def_readonly("vwap", &Snapshot::vwap)
        .def_readonly("volume", &Snapshot::volume)
        .def_readonly("trade_imbalance", &Snapshot::trade_imbalance)
        .def_readonly("volatility", &Snapshot::volatility)
        .def_readonly("trade_rate", &Snapshot::trade_rate)
        .def_readonly("vpin", &Snapshot::vpin)
        .def_readonly("vpin_bucket_volume", &Snapshot::vpin_bucket_volume)
        .def_readonly("vpin_buckets", &Snapshot::vpin_buckets)
        .def_readonly("trades_in_window", &Snapshot::trades_in_window)
        .def_readonly("total_trades", &Snapshot::total_trades)
        .def("as_dict", [](const Snapshot& s) {
            py::dict d;
            d["ts"] = s.ts;
            d["last_price"] = s.last_price;
            d["mid"] = s.mid;
            d["spread_bps"] = s.spread_bps;
            d["quote_imbalance"] = s.quote_imbalance;
            d["vwap"] = s.vwap;
            d["volume"] = s.volume;
            d["trade_imbalance"] = s.trade_imbalance;
            d["volatility"] = s.volatility;
            d["trade_rate"] = s.trade_rate;
            d["vpin"] = s.vpin;
            d["vpin_bucket_volume"] = s.vpin_bucket_volume;
            d["vpin_buckets"] = s.vpin_buckets;
            d["trades_in_window"] = s.trades_in_window;
            d["total_trades"] = s.total_trades;
            return d;
        });

    py::class_<AnalyticsEngine>(m, "AnalyticsEngine")
        .def(py::init<double, double, std::size_t>(),
             py::arg("window_seconds") = 60.0,
             py::arg("vpin_bucket_volume") = 0.0,
             py::arg("vpin_num_buckets") = 50,
             "vpin_bucket_volume <= 0 auto-calibrates from the first window of trading")
        .def("on_trade",
             [](AnalyticsEngine& e, double ts, double price, double size, int side) {
                 Trade t;
                 t.ts = ts;
                 t.price = price;
                 t.size = size;
                 t.side = side > 0 ? Side::Buy : side < 0 ? Side::Sell : Side::Unknown;
                 e.on_trade(t);
             },
             py::arg("ts"), py::arg("price"), py::arg("size"), py::arg("side") = 0,
             "side: +1 buyer was the aggressor, -1 seller was, 0 unknown")
        .def("on_quote",
             [](AnalyticsEngine& e, double ts, double bid, double ask,
                double bid_size, double ask_size) {
                 e.on_quote(Quote{ts, bid, ask, bid_size, ask_size});
             },
             py::arg("ts"), py::arg("bid"), py::arg("ask"),
             py::arg("bid_size") = 0.0, py::arg("ask_size") = 0.0)
        .def("snapshot", &AnalyticsEngine::snapshot)
        .def("reset", &AnalyticsEngine::reset)
        .def_property_readonly("window_seconds", &AnalyticsEngine::window_seconds);
}
