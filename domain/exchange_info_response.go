package domain

type ExchangeInfoResponse struct {
	Symbols []Info `json:"symbols"`
}
type Info struct {
	Symbol       string `json:"symbol"`
	Pair         string `json:"pair"`
	ContractType string `json:"contractType"`
	DeliveryDate int64  `json:"deliveryDate"`
	OnboardDate  int64  `json:"onboardDate"`
	Status       string `json:"status"`
	//MaintMarginPercent    string   `json:"maintMarginPercent"`
	//RequiredMarginPercent string   `json:"requiredMarginPercent"`
	//BaseAsset             string   `json:"baseAsset"`
	//QuoteAsset            string   `json:"quoteAsset"`
	//MarginAsset           string   `json:"marginAsset"`
	//PricePrecision        int      `json:"pricePrecision"`
	//QuantityPrecision     int      `json:"quantityPrecision"`
	//BaseAssetPrecision    int      `json:"baseAssetPrecision"`
	//QuotePrecision        int      `json:"quotePrecision"`
	//UnderlyingType        string   `json:"underlyingType"`
	//UnderlyingSubType     []string `json:"underlyingSubType"`
	//TriggerProtect        string   `json:"triggerProtect"`
	//LiquidationFee        string   `json:"liquidationFee"`
	//MarketTakeBound       string   `json:"marketTakeBound"`
	//MaxMoveOrderLimit     int      `json:"maxMoveOrderLimit"`
	//Filters               []struct {
	//	MaxPrice            string `json:"maxPrice,omitempty"`
	//	FilterType          string `json:"filterType"`
	//	TickSize            string `json:"tickSize,omitempty"`
	//	MinPrice            string `json:"minPrice,omitempty"`
	//	StepSize            string `json:"stepSize,omitempty"`
	//	MinQty              string `json:"minQty,omitempty"`
	//	MaxQty              string `json:"maxQty,omitempty"`
	//	Limit               int    `json:"limit,omitempty"`
	//	Notional            string `json:"notional,omitempty"`
	//	MultiplierDown      string `json:"multiplierDown,omitempty"`
	//	MultiplierUp        string `json:"multiplierUp,omitempty"`
	//	MultiplierDecimal   string `json:"multiplierDecimal,omitempty"`
	//	PositionControlSide string `json:"positionControlSide,omitempty"`
	//} `json:"filters"`
	//OrderTypes  []string `json:"orderTypes"`
	//TimeInForce []string `json:"timeInForce"`
}
