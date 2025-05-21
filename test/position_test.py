import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.position import Position
import unittest
from core.position import Position  # Adjust to match your actual import
from types import SimpleNamespace

class TestPosition(unittest.TestCase):

    def setUp(self):
        self.pos = Position(symbol='AAPL')

    def create_fill_event(self, direction, quantity, price, commission=0.0, slippage=0.0):
        return SimpleNamespace(
            direction=direction,
            quantity=quantity,
            fill_price=price,
            commission=commission,
            slippage=slippage
        )

    def test_initial_state(self):
        self.assertEqual(self.pos.quantity, 0)
        self.assertEqual(self.pos.avg_cost, 0)
        self.assertEqual(self.pos.realized_pnl, 0)

    def test_buy_fill(self):
        fill = self.create_fill_event('BUY', 10, 100.0, commission=1.0, slippage=0.5)
        self.pos.update_fill(fill)
        self.assertEqual(self.pos.quantity, 10)
        self.assertAlmostEqual(self.pos.avg_cost, 100.15)  # (100*10 + 1 + 0.5) / 10
        self.assertEqual(self.pos.realized_pnl, 0)
        self.assertAlmostEqual(self.pos.cumulated_commission, 1.0)
        self.assertAlmostEqual(self.pos.cumulated_slippage, 0.5)

    def test_buy_then_sell(self):
        self.pos.update_fill(self.create_fill_event('BUY', 10, 100.0, commission=0, slippage=0))
        self.pos.update_fill(self.create_fill_event('SELL', 5, 110.0, commission=1.0, slippage=0.5))

        self.assertEqual(self.pos.quantity, 5)
        self.assertAlmostEqual(self.pos.realized_pnl, (10 * 5) - 1.0 - 0.5)
        self.assertAlmostEqual(self.pos.cumulated_commission, 1.0)
        self.assertAlmostEqual(self.pos.cumulated_slippage, 0.5)

    def test_sell_entire_position(self):
        self.pos.update_fill(self.create_fill_event('BUY', 10, 50))
        self.pos.update_fill(self.create_fill_event('SELL', 10, 55))

        self.assertEqual(self.pos.quantity, 0)
        self.assertEqual(self.pos.avg_cost, 0.0)
        self.assertAlmostEqual(self.pos.realized_pnl, 50)  # (55 - 50) * 10

    def test_sell_more_than_held(self):
        self.pos.update_fill(self.create_fill_event('BUY', 5, 100))
        result = self.pos.update_fill(self.create_fill_event('SELL', 10, 105))
        self.assertFalse(result)
        self.assertEqual(self.pos.quantity, 5)  # Should remain unchanged

    def test_zero_quantity_fill(self):
        result = self.pos.update_fill(self.create_fill_event('BUY', 0, 100))
        self.assertEqual(self.pos.quantity, 0)

    def test_invalid_direction(self):
        fill = self.create_fill_event('HOLD', 10, 100)
        result = self.pos.update_fill(fill)
        self.assertFalse(result)

    def test_market_value(self):
        self.pos.update_fill(self.create_fill_event('BUY', 10, 50))
        self.assertEqual(self.pos.market_value(55), 550)

    def test_unrealized_pnl(self):
        self.pos.update_fill(self.create_fill_event('BUY', 10, 100))
        self.assertEqual(self.pos.unrealized_pnl(105), 50)

if __name__ == '__main__':
    unittest.main()
