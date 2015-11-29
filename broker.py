from nlib import *
import pickle
import sys
import datetime
import random
import copy

class Broker(object):
    
    def get_client_data(self):
        old_client_data = pickle.load(open(self.filename))
        
        clients = [client for client in old_client_data]
        clients.sort()
        self.clients = clients#[:10]
        print 'client list is done loading.'
        
        stocks = []
        for client in self.clients:
            stocks +=  [stock for stock in old_client_data[client]]
        stocks = set(stocks)
        stocks -= set(['BRK.B','WAG'])
        stocks = list(stocks)
        stocks.sort()
        self.stocks = stocks#[:10]
        print 'stock list is done loading.'
        
        client_data = {client:{} for client in self.clients}
        for client in self.clients:
            old_stocks = [stock for stock in old_client_data[client]]
            stocks = list(set(old_stocks) & set(self.stocks))
            for stock in stocks:
                client_data[client][stock] = old_client_data[client][stock]
        self.client_data = client_data
        print 'erroneous stocks are removed.'
    
    def get_stock_data(self):
        last_day = datetime.date(2015,3,31)
        stock_data = PersistentDictionary('stock_data.db')
        for stock in self.stocks:
            if stock not in stock_data:
                stock_data[stock] = YStock(stock).historical(stop=last_day)[-self.n_past_days:]
            print stock, 'data is downloaded.'
                
        prices = {}
        returns = {stock:[] for stock in self.stocks}
        i = 1
        while i <= self.n_past_days:
            for stock in self.stocks:
                daily_stock_data = stock_data[stock][-i]
                if i == 1:
                    prices[stock] = daily_stock_data['adjusted_close']
                returns[stock].append(daily_stock_data['log_return'])
            i += 1
        self.prices = prices        
        self.returns = returns        
                
    def simulate_once(self):
        days = [random.randint(0,self.n_past_days-1) for i in range(self.n_future_days)]
        future_prices = {}
        for stock in self.stocks:
            future_price = self.prices[stock]
            for day in days:
                future_price *= exp(self.returns[stock][day])
            future_prices[stock] = future_price
        return future_prices

    def simulate_many(self, ap=0.01, rp=0.01, ns=100):
        results = {stock:[] for stock in self.stocks}
        s1 = {stock:0.0 for stock in self.stocks}
        s2 = {stock:0.0 for stock in self.stocks}
        mu = {stock:0.0 for stock in self.stocks}
        dmu = {stock:0.0 for stock in self.stocks}
        self.convergence = False

        for k in xrange(1,ns+1):
            result = self.simulate_once()
            for stock in self.stocks:
                results[stock].append(result[stock])
                s1[stock] += result[stock]
                s2[stock] += result[stock]**2
                mu[stock] = s1[stock]/k
                dmu[stock] = sqrt((s2[stock]/k-mu[stock]**2)/k)
            if k > 10:
                flag = 0
                for stock in self.stocks:
                    if dmu[stock] < max(ap,abs(mu[stock])*rp):
                        flag += 1
                if flag == len(self.stocks):
                    self.convergence = True
                    break
        self.n_times = k
        self.results = results

    def quantile(self,data):
        data = copy.copy(data)
        data.sort()
        index = int(len(data)*0.01*self.confidence+0.999)
        if index < 5 or len(data)-index < 5:
            raise ArithmeticError('not enough data, not reliable')
        return data[index]
        
    def print_client_var(self):
        total_var = 0
        for client in self.clients:
            profits = []
            for k in range(self.n_times):
                profit = 0.0
                for stock in self.client_data[client]:
                    today_price = self.prices[stock]
                    future_price = self.results[stock][k]
                    n_share = self.client_data[client][stock]
                    profit += (future_price-today_price)*n_share
                profits.append(profit)
            var = int(self.quantile(profits))
            total_var += var
            print client, var
        print 'cumulative naive', total_var
    
    def print_house_var(self):
        n_shares = {}
        for stock in self.stocks:
            n_share = 0
            for client in self.clients:
                if stock in self.client_data[client]:
                    n_share += self.client_data[client][stock]
            n_shares[stock] = n_share
        
        total_profits = []
        for k in range(self.n_times):
            total_profit = 0.0
            for stock in self.stocks:
                today_price = self.prices[stock]
                future_price = self.results[stock][k]
                total_profit += (future_price-today_price)*n_shares[stock]
            total_profits.append(total_profit)
        print 'cumulative', int(self.quantile(total_profits))

def main():
    broker = Broker()
    broker.filename = str(sys.argv[1])
    broker.n_past_days = 250
    broker.n_future_days = int(sys.argv[2])
    broker.confidence = float(sys.argv[3])    
    broker.get_client_data()
    broker.get_stock_data()
    broker.simulate_many(ap=0.1,rp=0.0,ns=10**5)
    print 'simulated', broker.n_times, 'times'
    print 'results convergent?', broker.convergence
    print '----------------'
    broker.print_client_var()
    broker.print_house_var()

main()