package carbs

import (
	"os"
	"testing"
)

/*
func mkCar() (string, error) {
	f, err := ioutil.TempFile(os.TempDir(), "car")
	if err != nil {
		return "", err
	}
	defer f.Close()

	ds := mockNodeGetter{
		Nodes: make(map[cid.Cid]format.Node),
	}
	type linker struct {
		Name  string
		Links []*format.Link
	}
	cbornode.RegisterCborType(linker{})

	children := make([]format.Node, 0, 10)
	childLinks := make([]*format.Link, 0, 10)
	for i := 0; i < 10; i++ {
		child, _ := cbornode.WrapObject([]byte{byte(i)}, multihash.SHA2_256, -1)
		children = append(children, child)
		childLinks = append(childLinks, &format.Link{Name: fmt.Sprintf("child%d", i), Cid: child.Cid()})
	}
	b, err := cbornode.WrapObject(linker{Name: "root", Links: childLinks}, multihash.SHA2_256, -1)
	if err != nil {
		return "", fmt.Errorf("couldn't make cbor node: %v", err)
	}
	ds.Nodes[b.Cid()] = b

	if err := car.WriteCar(context.Background(), &ds, []cid.Cid{b.Cid()}, f); err != nil {
		return "", err
	}

	return f.Name(), nil
}
*/

func TestIndexRT(t *testing.T) {
	/*
		carFile, err := mkCar()
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(carFile)
	*/
	// TODO use temporari directory to run tests taht work with OS file system to avoid accidental source code modification
	carFile := "testdata/test.car"

	cf, err := Load(carFile, false)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(carFile + ".idx")

	r, err := cf.Roots()
	if err != nil {
		t.Fatal(err)
	}
	if len(r) != 1 {
		t.Fatalf("unexpected number of roots: %d", len(r))
	}
	if _, err := cf.Get(r[0]); err != nil {
		t.Fatalf("failed get: %v", err)
	}

	idx, err := Restore(carFile)
	if err != nil {
		t.Fatalf("failed restore: %v", err)
	}
	if idx, err := idx.Get(r[0]); idx == 0 || err != nil {
		t.Fatalf("bad index: %d %v", idx, err)
	}
}
