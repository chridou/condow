pub struct CondowStream {
    n_parts: usize,
    is_ordered: bool,
}

impl CondowStream {
    pub fn empty() -> Self {
        CondowStream {
            n_parts: 0,
            is_ordered: true,
        }
    }

    pub fn n_parts(&self) -> usize {
        self.n_parts
    }
    
    pub fn is_ordered(&self) -> bool {
        self.is_ordered
    } 

}
