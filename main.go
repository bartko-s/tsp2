package main

import (
    "bufio"
    "errors"
    "flag"
    "fmt"
    "io"
    "log"
    "math"
    "math/rand"
    "os"
    "runtime"
    "runtime/pprof"
    "runtime/trace"
    "strconv"
    "strings"
    "sync"
    "time"
)

var cpuProfile = flag.String("cpu-profile", "", "write cpu profile to `file`")
var traceLog = flag.String("trace-log", "", "write trace log to `file`")
var debug = flag.Bool("debug", false, "show debug info")
var duration = flag.String("duration", "", "max execution time for example 500ms or 60s")

func showDebugMessage(msg string) {
    if *debug == true {
        fmt.Println(msg)
    }
}

func showExecutionTime(msg string, startTime time.Time) {
    showDebugMessage(fmt.Sprintf("%s: %d ms", msg, time.Now().Sub(startTime)/(1000*1000)))
}

func convertStringToInt(str string) int {
    v, err := strconv.ParseInt(str, 10, 0)
    if err != nil {
        fmt.Fprintln(os.Stderr, "reading standard input:", err)
    }
    return int(v)
}

type Route struct {
    regions     []string
    totalCost   int
    costs       Costs
    defaultCost int
}

var routePool *sync.Pool

func initRoutePool() {
    routePool = &sync.Pool{
        New: func() interface{} {
            return new(Route)
        },
    }
}

func NewRoute(costs Costs) *Route {
    var regions []string
    route := routePool.Get().(*Route)
    route.regions = regions
    route.totalCost = 0
    route.costs = costs
    route.defaultCost = 10 * 1000
    return route
}

func (r *Route) Copy() *Route {
    route := routePool.Get().(*Route)
    route.regions = route.regions[:0]
    route.regions = append(route.regions, r.regions...)
    route.totalCost = r.totalCost
    route.costs = r.costs
    route.defaultCost = r.defaultCost
    return route
}

func (r *Route) AddRegion(region string) {
    regionsCount := len(r.regions)
    if regionsCount > 0 {
        if cost, ok := r.costs.GetCost(r.regions[regionsCount-1], region, regionsCount); ok == nil {
            r.totalCost = r.totalCost + cost
        } else {
            r.totalCost = r.totalCost + r.defaultCost // default cost for unknown fly
        }
    }
    r.regions = append(r.regions, region)
}

func (r *Route) Change(newRegion string, atDay int) {
    costDelta := 0
    if cost, ok := r.costs.GetCost(r.regions[atDay-1], r.regions[atDay], atDay); ok == nil {
        costDelta = costDelta - cost
    } else {
        costDelta = costDelta - r.defaultCost
    }
    if cost, ok := r.costs.GetCost(r.regions[atDay-1], newRegion, atDay); ok == nil {
        costDelta = costDelta + cost
    } else {
        costDelta = costDelta + r.defaultCost
    }

    if atDay < len(r.regions)-1 {
        if cost, ok := r.costs.GetCost(r.regions[atDay], r.regions[atDay+1], atDay+1); ok == nil {
            costDelta = costDelta - cost
        } else {
            costDelta = costDelta - r.defaultCost
        }
        if cost, ok := r.costs.GetCost(newRegion, r.regions[atDay+1], atDay+1); ok == nil {
            costDelta = costDelta + cost
        } else {
            costDelta = costDelta + r.defaultCost
        }
    }

    r.regions[atDay] = newRegion
    r.totalCost = r.totalCost + costDelta
}

type Regions struct {
    groups              map[string][]string
    regionBelongToGroup map[string]string
}

func NewRegions() Regions {
    return Regions{
        make(map[string][]string),
        make(map[string]string),
    }
}

func (r *Regions) AddRegion(group string, regions []string) {
    _, ok := r.groups[group]
    if ok == false {
        r.groups[group] = regions
    } else {
        r.groups[group] = append(r.groups[group], regions...)
    }

    for _, elm := range regions {
        r.regionBelongToGroup[elm] = group
    }
}

func (r *Regions) GetAllRegions(region string) []string {
    return r.groups[r.regionBelongToGroup[region]]
}

type Costs struct {
    costs map[string]int
    lock  sync.Mutex
}

func NewCosts() Costs {
    return Costs{
        make(map[string]int),
        sync.Mutex{},
    }
}

func (c *Costs) CreateKey(from string, to string, day int) string {
    return from + "@:@" + to + "@:@" + string(day)
}

func (c *Costs) AddCost(from string, to string, day int, cost int) {
    c.lock.Lock()
    c.costs[c.CreateKey(from, to, day)] = cost
    c.lock.Unlock()
}

func (c *Costs) GetCost(from string, to string, day int) (int, error) {
    if item, ok := c.costs[c.CreateKey(from, to, day)]; ok {
        return item, nil
    }
    if item, ok := c.costs[c.CreateKey(from, to, 0)]; ok {
        return item, nil
    } else {
        return -1, errors.New("cannot find cost for location")
    }
}

type CostItem struct {
    from string
    to   string
    day  int
    cost int
}

func parseInput(r io.Reader) (int, int, string, Costs, Regions) {
    var workersCount int
    if runtime.NumCPU() <= 2 {
        workersCount = 1
    } else {
        workersCount = runtime.NumCPU() - 1
    }
    wg := sync.WaitGroup{}
    costCh := make(chan *CostItem, 10000)

    costItemPool := sync.Pool{
        New: func() interface{} {
            return new(CostItem)
        },
    }

    worker := func(costs *Costs, ch chan *CostItem, itemPool *sync.Pool, wg *sync.WaitGroup) {
        for item := range ch {
            costs.AddCost(item.from, item.to, item.day, item.cost)
            itemPool.Put(item)
        }
        wg.Done()
    }

    scanner := bufio.NewScanner(r)

    var totalRegionsCount int
    var visitRegionsCount int
    var startRegion string
    var costs = NewCosts()
    var regionGroups = NewRegions()

    for i := 0; i < workersCount; i++ {
        wg.Add(1)
        go worker(&costs, costCh, &costItemPool, &wg)
    }

    separator := " "
    i := 1
    var regionGroupName string
    for scanner.Scan() {
        line := scanner.Text()

        if i == 1 {
            s := strings.Split(line, separator)
            visitRegionsCount = convertStringToInt(s[0])
            startRegion = s[1]
        }
        if 2 <= i && ((visitRegionsCount*2)+1) >= i {
            if i%2 == 0 {
                regionGroupName = line
            }
            if i%2 > 0 {
                regions := strings.Split(line, separator)
                regionGroups.AddRegion(regionGroupName, regions)
                totalRegionsCount += len(regions)
            }
        }
        if i > (visitRegionsCount*2)+1 {
            s := strings.Split(line, separator)
            costItem := costItemPool.Get().(*CostItem)
            costItem.from = s[0]
            costItem.to = s[1]
            costItem.day = convertStringToInt(s[2])
            costItem.cost = convertStringToInt(s[3])
            costCh <- costItem
        }

        i = i + 1
    }
    close(costCh)
    if err := scanner.Err(); err != nil {
        fmt.Fprintln(os.Stderr, "reading standard input:", err)
    }

    wg.Wait()
    return totalRegionsCount, visitRegionsCount, startRegion, costs, regionGroups
}

func createInitialState(regions Regions, costs Costs, startRegion string) *Route {
    route := NewRoute(costs)
    route.AddRegion(startRegion)

    for _, regions := range regions.groups {
        isStartLocation := false
        for _, r := range regions {
            if startRegion == r {
                isStartLocation = true
                break
            }
        }
        if isStartLocation == false {
            route.AddRegion(regions[rand.Intn(len(regions))])
        }
    }

    route.AddRegion(startRegion)

    return route
}

func renderOutput(route *Route, costs Costs) {
    state := route.regions

    var paths []string
    totalCost := 0

    from := state[0]
    for i := 1; i < len(state); i++ {
        cost, e := costs.GetCost(from, state[i], i)
        if e != nil {
            cost = -1
        }

        paths = append(paths, fmt.Sprintf("%s %s %d %d", from, state[i], i, cost))
        from = state[i]
        totalCost = totalCost + cost
    }

    fmt.Println(totalCost)
    for _, s := range paths {
        fmt.Println(s)
    }
}

type BestRoute struct {
    route *Route
    sync.Mutex
}

func NewBestRoute(route *Route) BestRoute {
    return BestRoute{
        route,
        sync.Mutex{},
    }
}

type TravellingSalesmanProblem struct {
    Tmax         float64
    Tmin         float64
    state        *Route
    costs        Costs
    regions      Regions
    endTime      time.Time
    hasMultiZone bool
}

func NewTravellingSalesmanProblem(initialState *Route, costs Costs, regions Regions, hasMultiZone bool) TravellingSalesmanProblem {
    var endTime time.Time
    t := TravellingSalesmanProblem{
        25000.0,
        2.5,
        initialState,
        costs,
        regions,
        endTime,
        hasMultiZone,
    }
    return t
}

func (t *TravellingSalesmanProblem) CountEnergy() int {
    return t.state.totalCost
}

func (t *TravellingSalesmanProblem) SwapZone(route *Route) {
    // Swaps two cities in the route. Do not move first region and last
    regionsCount := len(route.regions)
    for r := 1; r <= 10; r++ {
        ai := rand.Intn(regionsCount-2) + 1
        bi := rand.Intn(regionsCount-2) + 1
        if ai > bi {
            ai, bi = bi, ai
        }
        if ai != bi {
            aRegion := route.regions[ai]
            bRegion := route.regions[bi]

            if _, ok := t.costs.GetCost(route.regions[ai-1], bRegion, ai); ok != nil {
                continue
            }

            if ai == bi-1 {
                if _, ok := t.costs.GetCost(bRegion, aRegion, bi); ok != nil {
                    continue
                }
            } else {
                if _, ok := t.costs.GetCost(route.regions[bi-1], aRegion, bi); ok != nil {
                    continue
                }
            }

            route.Change(aRegion, bi)
            route.Change(bRegion, ai)
            break
        }
    }
}

func (t *TravellingSalesmanProblem) SwapRegionInZone(route *Route) {
    for r := 1; r <= 10; r++ {
        // do no change first zone
        a := rand.Intn(len(route.regions)-1) + 1
        region := route.regions[a]
        regions := t.regions.GetAllRegions(region)
        newRegion := ""

        count := len(regions)
        if count == 2 {
            if regions[0] != region {
                newRegion = regions[0]
            } else {
                newRegion = regions[1]
            }
        } else if count > 2 {
            newRegion = regions[rand.Intn(len(regions))]
            if newRegion == region {
                continue
            }
        }

        if newRegion != "" {
            if _, ok := t.costs.GetCost(route.regions[a-1], newRegion, a); ok != nil {
                continue
            }

            route.Change(newRegion, a)
            break
        }

    }
}

func (t *TravellingSalesmanProblem) Move(step int, route *Route) {
    if t.hasMultiZone {
        if step%2 == 0 {
            t.SwapZone(route)
        } else {
            t.SwapRegionInZone(route)
        }
    } else {
        t.SwapZone(route)
    }
}

func (t *TravellingSalesmanProblem) Resolve() *Route {
    bestState := NewBestRoute(t.state.Copy())
    startTime := time.Now()
    duration := t.endTime.Sub(startTime)

    step := 0

    resolve := func(currentState *Route, wg *sync.WaitGroup) {
        Tfactor := math.Log(t.Tmax/t.Tmin) * -1

        T := t.Tmax
        prevState := currentState.Copy()
        localBestEnergy := currentState.totalCost

        currentTime := time.Now()
        stop := false
        for stop == false {
            if step%1000 == 0 {
                currentTime = time.Now()
                if t.endTime.Sub(currentTime) < 0 {
                    stop = true
                }
            }

            T = t.Tmax * math.Exp(Tfactor*float64(currentTime.Sub(startTime))/float64(duration))
            t.Move(step, currentState)
            dE := currentState.totalCost - prevState.totalCost

            if currentState.totalCost <= bestState.route.totalCost {
                bestState.Lock()
                routePool.Put(bestState.route)
                bestState.route = currentState.Copy()
                bestState.Unlock()
                localBestEnergy = currentState.totalCost
            }

            if dE > 0.0 && math.Exp((float64(dE)*-1)/T) < rand.Float64() {
                // restore prev state
                routePool.Put(currentState)
                currentState = prevState.Copy()
            } else {
                // accept new state
                routePool.Put(prevState)
                prevState = currentState.Copy()
            }

            // sync gorountine best state
            if localBestEnergy > bestState.route.totalCost {
                routePool.Put(currentState)
                bestState.Lock()
                currentState = bestState.route.Copy()
                bestState.Unlock()
                localBestEnergy = currentState.totalCost
            }

            step++
        }
        routePool.Put(currentState)
        wg.Done()
    }

    workersCount := runtime.NumCPU()
    wg := sync.WaitGroup{}
    for i := 1; i <= workersCount; i++ {
        wg.Add(1)
        go resolve(t.state.Copy(), &wg)
    }
    wg.Wait()

    showDebugMessage(fmt.Sprintf("Steps: %d", step))
    t.state = bestState.route
    return t.state
}

func (t *TravellingSalesmanProblem) auto(steps int) (float64, float64) {
    bestState := t.state.Copy()
    bestEnergy := t.CountEnergy()

    run := func(T float64, steps int) (int, float64, float64) {
        E := t.CountEnergy()
        prevState := t.state.Copy()
        prevEnergy := E
        accepts, improves := 0, 0
        for i := 1; i <= steps; i++ {
            t.Move(i, t.state)
            E = t.CountEnergy()
            dE := E - prevEnergy

            if E < bestEnergy {
                routePool.Put(bestState)
                bestState = t.state.Copy()
                bestEnergy = E
            }

            if dE > 0.0 && math.Exp((float64(dE)*-1)/T) < float64(float64(rand.Intn(100000))/100000) {
                routePool.Put(t.state)
                t.state = prevState.Copy()
                E = prevEnergy
            } else {
                accepts++
                if dE < 0.0 {
                    improves++
                }
                routePool.Put(prevState)
                prevState = t.state.Copy()
                prevEnergy = E
            }
        }
        return E, float64(accepts) / float64(steps), float64(improves) / float64(steps)
    }

    step := 0

    T := 0.0
    E := t.CountEnergy()
    for T == 0.0 {
        step++
        t.Move(step, t.state)
        T = math.Abs(float64(t.CountEnergy()) - float64(E))
    }

    E, acceptance, improvement := run(T, steps)

    step = step + steps
    for acceptance > 0.98 {
        T = T / 1.5
        E, acceptance, improvement = run(T, steps)
        step = step + steps
    }
    for acceptance < 0.98 {
        T = T * 1.5
        E, acceptance, improvement = run(T, steps)
        step = step + steps
    }
    Tmax := T

    for improvement > 0.0 {
        T = T / 1.5
        E, acceptance, improvement = run(T, steps)
        step = step + steps
    }
    Tmin := T

    t.state = bestState

    return Tmax, Tmin
}

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())
    rand.Seed(time.Now().UnixNano())
    var startTime = time.Now()

    initRoutePool()

    flag.Parse()
    if *cpuProfile != "" {
        f, err := os.Create(*cpuProfile)
        if err != nil {
            log.Fatal("could not create CPU profile: ", err)
        }
        if err := pprof.StartCPUProfile(f); err != nil {
            log.Fatal("could not start CPU profile: ", err)
        }
        defer pprof.StopCPUProfile()
    }

    if *traceLog != "" {
        f, err := os.Create(*traceLog)
        if err != nil {
            panic(err)
        }
        defer f.Close()

        err = trace.Start(f)
        if err != nil {
            panic(err)
        }
        defer trace.Stop()
    }

    var file = os.Stdin
    //file, _ := os.Open("data/0.in.txt")

    //file, _ := os.Open("data/1.in.txt")
    //file, _ := os.Open("data/2.in.txt")
    //file, _ := os.Open("data/3.in.txt")
    //file, _ := os.Open("data/4.in.txt")

    pt := time.Now()
    totalRegionsCount, zones, startRegion, costs, regions := parseInput(file)
    showExecutionTime("File Parse time", pt)

    initialState := createInitialState(regions, costs, startRegion)

    hasMultiRegion := totalRegionsCount > zones
    T := NewTravellingSalesmanProblem(initialState, costs, regions, hasMultiRegion)
    var maxDuration string
    if *duration != "" {
        maxDuration = *duration
    } else if zones <= 20 && totalRegionsCount < 50 {
        maxDuration = "1300ms"
    } else if zones <= 100 && totalRegionsCount < 200 {
        maxDuration = "4800ms"
    } else {
        maxDuration = "14700ms"
    }

    duration, err := time.ParseDuration(maxDuration)
    if err != nil {
        panic(err)
    }
    T.endTime = startTime.Add(duration)

    T.Tmax, T.Tmin = T.auto(2000)
    rt := time.Now()
    resultState := T.Resolve()
    showExecutionTime("Resolve time", rt)

    showDebugMessage(fmt.Sprintf("Tmin: %f, Tmax %f", T.Tmin, T.Tmax))
    showExecutionTime("Total time", startTime)
    showDebugMessage(fmt.Sprintf("Total zones: %d, Visised %d", totalRegionsCount, zones))

    renderOutput(resultState, costs)
}
